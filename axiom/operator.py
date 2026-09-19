"""Typed, localhost-only operator controls for the paper research node.

This module deliberately has no shell or browser-provided command execution. The
control surface maps a small allowlist to existing in-process service APIs and
persists every requested action as a bounded audit record.
"""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import ipaddress
import json
import logging
import math
import os
from pathlib import Path
import re
import subprocess
import sqlite3
import sys
import threading
import time
import uuid
from typing import Any, Callable, Mapping

from .autonomous import AutonomousResearchProcessor
from .canary import (
    CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS,
    CanaryBlocked,
    CanaryService,
    CredentialStore,
    PolymarketClobV2Venue,
    _canary_adverse_acknowledged,
    _canary_authorization_current_selection_hash,
    _canary_canonical_selection_hash,
    _canary_selection_member_is_funded,
    _canary_selection_member_is_proposed,
    credential_fingerprint,
)
from .canary_positions import (
    RECOVERY_ACTION,
    RECOVERY_CONFIRMATION,
    RecoveryProfileError,
    normalize_recovery_profile,
    recovery_identifier,
)
from .canary_settings import CanarySettingsService
from .rolling_portfolio import RollingAdmissionPolicy, default_rolling_admission_policy
from .bootstrap import BTC_HISTORY_START, HistoricalBootstrapper
from .crypto_universe import load_crypto_universe
from .data import BinanceAdapter
from .domain import utc_now
from .node import (
    EXECUTION_PROFILE_ENV,
    ISOLATED_EXECUTION_PROFILE,
    PRODUCTION_EXECUTION_PROFILE,
    _pid_alive,
    _pid_matches_node,
    normalized_execution_profile,
)

_LOGGER = logging.getLogger(__name__)


DEFAULT_HERMES_JOB_ID = "f1d27bf8c27a"

BOOTSTRAP_JOB_NAME = "crypto-universe-bootstrap"
HERMES_STATE_NAME = "hermes-control"
# The configured Hermes job is an external reference only.  The operator has
# no local verifier for that process, so scheduler state must never be exposed
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
        "canary.recover_entry",
        "canary.settings.save_draft",
        "canary.settings.activate_draft",
        "risk.settings.save_draft",
        "risk.settings.activate_draft",
        "rolling.admission.review",
        "rolling.admission.activate",
        "rolling.policy.review",
        "rolling.policy.activate",
        "admission_policy.review",
        "admission_policy.activate",
        "execution_authorization.review",
        "execution_authorization.activate",
        "execution_authorization.revoke",
        "exploratory.authorization.review",
        "exploratory.authorization.activate",
        "exploratory.authorization.revoke",
        "authorization.review",
        "exploratory.live.review_confirm",
        "authorization.revoke",
    }
)
_AUTHORIZATION_ACTION_ALIASES = {
    "execution_authorization.review": "execution_authorization.review",
    "execution_authorization.activate": "execution_authorization.activate",
    "execution_authorization.revoke": "execution_authorization.revoke",
    "exploratory.authorization.review": "execution_authorization.review",
    "exploratory.authorization.activate": "execution_authorization.activate",
    "exploratory.authorization.revoke": "execution_authorization.revoke",
    "authorization.review": "execution_authorization.review",
    "authorization.activate": "execution_authorization.activate",
    "authorization.revoke": "execution_authorization.revoke",
}
_EXPLORATORY_LIVE_ACTION_ALIASES = {
    "exploratory.live.review_confirm": "exploratory.live.review_confirm",
    "exploratory_live.review_confirm": "exploratory.live.review_confirm",
    "canary.exploratory_live.review_confirm": "exploratory.live.review_confirm",
}
_ROLLING_ACTION_ALIASES = {
    "rolling.admission.review": "rolling.admission.review",
    "rolling.policy.review": "rolling.admission.review",
    "admission_policy.review": "rolling.admission.review",
    "rolling.admission.activate": "rolling.admission.activate",
    "rolling.policy.activate": "rolling.admission.activate",
    "admission_policy.activate": "rolling.admission.activate",
}
_SETTINGS_ACTION_ALIASES = {
    "canary.settings.save_draft": "risk.settings.save_draft",
    "risk.settings.save_draft": "risk.settings.save_draft",
    "canary.settings.activate_draft": "risk.settings.activate_draft",
    "risk.settings.activate_draft": "risk.settings.activate_draft",
}
_ACTION_ALIASES = {
    **_ROLLING_ACTION_ALIASES,
    **_SETTINGS_ACTION_ALIASES,
    **_AUTHORIZATION_ACTION_ALIASES,
    **_EXPLORATORY_LIVE_ACTION_ALIASES,
}
_CONFIRMATIONS = {
    "canary.eligibility.mark": "MARK CANARY ELIGIBLE",
    "canary.arm": "ARM",
    "canary.enable_auto": "ENABLE AUTO CANARY",
    "canary.disarm": "DISARM",
    "canary.kill": "KILL",
    RECOVERY_ACTION: RECOVERY_CONFIRMATION,
    "canary.settings.save_draft": "SAVE RISK SETTINGS DRAFT",
    "canary.settings.activate_draft": "ACTIVATE RISK SETTINGS DRAFT",
    "risk.settings.save_draft": "SAVE RISK SETTINGS DRAFT",
    "risk.settings.activate_draft": "ACTIVATE RISK SETTINGS DRAFT",
    "rolling.admission.review": "REVIEW ROLLING ADMISSION POLICY",
    "rolling.admission.activate": "ACTIVATE ROLLING ADMISSION POLICY",
    "rolling.policy.review": "REVIEW ROLLING ADMISSION POLICY",
    "rolling.policy.activate": "ACTIVATE ROLLING ADMISSION POLICY",
    "admission_policy.review": "REVIEW ROLLING ADMISSION POLICY",
    "admission_policy.activate": "ACTIVATE ROLLING ADMISSION POLICY",
}
_CONFIRMATIONS["exploratory.live.review_confirm"] = "CONFIRM EXPLORATORY LIVE"
_CONFIRMATIONS.update(
    {
        "execution_authorization.review": "REVIEW EXPLORATORY AUTHORIZATION",
        "execution_authorization.activate": "ACTIVATE EXPLORATORY AUTHORIZATION",
        "execution_authorization.revoke": "REVOKE EXPLORATORY AUTHORIZATION",
    }
)
_ISOLATED_OPERATOR_BLOCKED_ACTIONS = frozenset(
    {
        "canary.connectivity_check",
        "canary.arm",
        "canary.enable_auto",
        "exploratory.live.review_confirm",
        "risk.settings.activate_draft",
    }
)
CANARY_CONNECTIVITY_CONFIG_KEY = "canary_connectivity_status"



_CONNECTIVITY_FAILURE_REASONS = {
    "CONNECTIVITY_CHECK_FAILED": "Connectivity check failed.",
    "CREDENTIALS_NOT_CONFIGURED": "Polymarket credentials are not configured.",
    "VENUE_REQUIRED": "A Polymarket venue is required for the connectivity check.",
    "CREDENTIAL_BINDING_MISSING": "Credential binding is missing.",
    "CREDENTIAL_BINDING_MISMATCH": "Credential binding does not match the configured account.",
    "OFFICIAL_POLYMARKET_SDK_NOT_INSTALLED": "The Polymarket SDK is not installed.",
    "UNSUPPORTED_POLYMARKET_SDK": "The installed Polymarket SDK version is unsupported.",
    "OFFICIAL_POLYMARKET_SDK_NOT_READONLY_COMPATIBLE": "The installed Polymarket SDK does not support safe read-only connectivity.",
    "GEOGRAPHICALLY_BLOCKED": "The current region is blocked from Polymarket access.",
    "GEOBLOCK_CHECK_FAILED": "The geographic access check failed.",
    "AUTHENTICATED_CONNECTIVITY_FAILED": "Authenticated connectivity failed.",
    "ACCOUNT_CHECK_FAILED": "The account check failed.",
    "BALANCE_RESPONSE_INVALID": "The balance response was invalid.",
    "INSUFFICIENT_BALANCE": "Available balance is below the active canary requirement.",
    "CANARY_ALLOWANCE_UNAVAILABLE": "Allowance information is unavailable.",
    "CANARY_ALLOWANCE_INSUFFICIENT": "Current allowance is below the active canary requirement.",
    "CANARY_SPENDER_UNAVAILABLE": "The canary allowance spender is unavailable.",
    "MARKET_CONNECTIVITY_FAILED": "Market connectivity failed.",
    "MARKET_NOT_ACCEPTING_ORDERS": "The market is not accepting orders.",
    "MARKET_OUTCOMES_UNAVAILABLE": "Market outcomes are unavailable.",
    "MARKET_OUTCOME_NOT_ALLOWED": "The requested market outcome is not allowed.",
    "MARKET_OUTCOME_ID_UNAVAILABLE": "The market outcome identifier is unavailable.",
    "VENUE_MINIMUM_EXCEEDS_CANARY_TARGET": "The venue minimum exceeds the active canary target.",
}
_CONNECTIVITY_FAILURE_CODES = frozenset(_CONNECTIVITY_FAILURE_REASONS)
_CONNECTIVITY_TEXT_LIMIT = 128
_CONNECTIVITY_MAX_FAILURES = 16
_CONNECTIVITY_SUPPORTED_SDK = "0.9"
_CONNECTIVITY_CODE = re.compile(r"^[A-Z0-9][A-Z0-9_.:-]{0,63}$")
_CONNECTIVITY_LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _./:-]{0,63}$")
_CONNECTIVITY_DECIMAL = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_CREDENTIAL_FINGERPRINT_RE = re.compile(r"^sha256:v1:[0-9a-f]{64}$")
_CONNECTIVITY_FINGERPRINT_UNSET = object()
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


def _connectivity_projection_is_fresh(checked_at: str) -> bool:
    try:
        now = _connectivity_checked_at(utc_now())
        if now is None:
            return False
        age_seconds = (
            datetime.fromisoformat(now) - datetime.fromisoformat(checked_at)
        ).total_seconds()
    except (TypeError, ValueError, OverflowError):
        return False
    return 0.0 <= age_seconds <= CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS


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




def _project_connectivity(
    value: Any,
    *,
    checked_at: Any = None,
    authoritative_credentials_configured: bool | None = None,
    authoritative_fingerprint: Any = _CONNECTIVITY_FINGERPRINT_UNSET,
) -> dict[str, Any]:
    """Project service diagnostics into the deliberately small operator schema.

    The service and venue may obtain credentials through different read paths.
    When the operator has an authoritative credential read, bind the public
    account projection to that read rather than persisting an arbitrary
    fingerprint supplied by a transport.
    """
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
    raw_credentials_configured = raw.get("credentials_configured")
    if not isinstance(raw_credentials_configured, bool):
        raw_credentials_configured = diagnostics.get("credentials_configured")
    if not isinstance(raw_credentials_configured, bool):
        raw_credentials_configured = credentials_raw.get("status") == "CONFIGURED"
    credentials_status = str(credentials_raw.get("status") or "").strip().upper()
    if credentials_status == "NOT CONFIGURED":
        raw_credentials_configured = False
    elif credentials_status == "CONFIGURED" and not isinstance(raw.get("credentials_configured"), bool):
        raw_credentials_configured = True
    credentials_configured = (
        authoritative_credentials_configured
        if isinstance(authoritative_credentials_configured, bool)
        else bool(raw_credentials_configured)
    )
    if not credentials_configured and "VENUE_REQUIRED" in failure_codes:
        # A venue cannot be constructed before credentials are configured; do
        # not report that derived prerequisite alongside its root blocker.
        failure_codes.remove("VENUE_REQUIRED")

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
    if not credentials_configured:
        authentication_raw = {}
    authentication_status = explicit_status(
        authentication_raw,
        pass_values=frozenset({"OK", "PASS", "PASSED", "SUCCESS"}),
        fail_values=frozenset({"FAILED", "FAIL", "ERROR"}),
    )
    if authentication_status is None and "AUTHENTICATED_CONNECTIVITY_FAILED" in failure_codes:
        authentication_status = "FAIL"
    if not credentials_configured:
        authentication_status = "SKIPPED"
    authentication = {"status": authentication_status or "SKIPPED"}

    account_raw = source("account")
    if not credentials_configured:
        account_raw = {}
    account_status = explicit_status(
        account_raw,
        pass_values=frozenset({"OK", "PASS", "PASSED", "SUCCESS"}),
        fail_values=frozenset({"FAILED", "FAIL", "ERROR"}),
    )
    if isinstance(account_raw.get("authenticated"), bool):
        account_status = "PASS" if account_raw["authenticated"] else "FAIL"
    if account_status is None and "ACCOUNT_CHECK_FAILED" in failure_codes:
        account_status = "FAIL"
    raw_fingerprint = account_raw.get("credential_fingerprint")
    account_fingerprint = (
        raw_fingerprint
        if isinstance(raw_fingerprint, str)
        and _CREDENTIAL_FINGERPRINT_RE.fullmatch(raw_fingerprint)
        else None
    )
    if authoritative_fingerprint is not _CONNECTIVITY_FINGERPRINT_UNSET:
        bound_fingerprint = (
            authoritative_fingerprint
            if isinstance(authoritative_fingerprint, str)
            and _CREDENTIAL_FINGERPRINT_RE.fullmatch(authoritative_fingerprint)
            else None
        )
        if (
            credentials_configured
            and bound_fingerprint is not None
            and account_fingerprint is not None
            and account_fingerprint != bound_fingerprint
        ):
            add_failure("CREDENTIAL_BINDING_MISMATCH")
        account_fingerprint = bound_fingerprint
    account = {
        "status": account_status or "SKIPPED",
        "wallet_type": _connectivity_text(account_raw.get("wallet_type")),
        "credential_fingerprint": account_fingerprint,
    }
    if credentials_configured and account_fingerprint is None:
        add_failure("CREDENTIAL_BINDING_MISSING")

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
    if not credentials_configured:
        balance_raw = {}
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
    if not credentials_configured:
        allowance_raw = {}
    allowance_value = str(allowance_raw.get("status") or "").strip().upper()
    if not credentials_configured:
        allowance_status = "SKIPPED"
    elif "CANARY_ALLOWANCE_INSUFFICIENT" in failure_codes or allowance_value == "INSUFFICIENT":
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
        "allowance",
        "sdk",
        "credentials",
        "authentication",
        "account",
        "geoblock",
        "balance",
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
    "geoblock": frozenset({"status", "country", "region"}),
    "account": frozenset({"status", "wallet_type", "credential_fingerprint"}),
    "allowance": frozenset({"status"}),
    "market": frozenset({"status"}),
    "order_book": frozenset({"status"}),
}


def _stored_connectivity_projection(
    value: Any,
    *,
    require_fresh: bool = False,
) -> dict[str, Any] | None:
    if not isinstance(value, Mapping) or set(value) != _CONNECTIVITY_PROJECTION_KEYS:
        return None
    checked_at = _connectivity_checked_at(value.get("checked_at"))
    if checked_at is None or value.get("checked_at") != checked_at:
        return None
    if require_fresh and not _connectivity_projection_is_fresh(checked_at):
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
            or value["account"]["status"] != "PASS"
            or value["account"]["credential_fingerprint"] is None
            or value["geoblock"]["status"] != "PASS"
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
def _blocked_connectivity_projection(
    code: Any = None,
    *,
    credentials_configured: bool | None = None,
    authoritative_fingerprint: Any = _CONNECTIVITY_FINGERPRINT_UNSET,
) -> dict[str, Any]:
    candidate = code.strip().upper() if isinstance(code, str) else ""
    safe_code = candidate if candidate in _CONNECTIVITY_FAILURE_CODES else "CONNECTIVITY_CHECK_FAILED"
    return _project_connectivity(
        {"ready": False, "failures": [safe_code], "live_execution": False},
        checked_at=utc_now(),
        authoritative_credentials_configured=credentials_configured,
        authoritative_fingerprint=authoritative_fingerprint,
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
_ROLLING_POLICY_ID_ALIASES = ("policy_id", "id", "draft_id")
_ROLLING_POLICY_VERSION_ALIASES = ("version", "policy_version", "draft_version")
_ROLLING_POLICY_HASH_ALIASES = ("config_hash", "policy_hash", "draft_hash")


def _rolling_identity_field(
    source: Mapping[str, Any],
    aliases: tuple[str, ...],
    *,
    required: bool,
) -> str:
    values: list[str] = []
    for alias in aliases:
        if alias not in source:
            continue
        raw = source.get(alias)
        text = str(raw).strip() if raw is not None else ""
        if not text:
            raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
        values.append(text)
    if values and len(set(values)) != 1:
        raise OperatorControlError("ROLLING_POLICY_IDENTITY_AMBIGUOUS")
    if required and not values:
        raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
    return values[0] if values else ""


def _rolling_policy_identity(
    value: Any,
    *,
    require_hash: bool = True,
) -> dict[str, str]:
    """Normalize a policy envelope and fail closed on nested identity conflicts."""
    if not isinstance(value, Mapping):
        raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")

    def fields(source: Mapping[str, Any]) -> dict[str, str]:
        id_aliases = (
            ("policy_id", "id")
            if any(alias in source for alias in ("policy_id", "id"))
            else ("draft_id",)
        )
        version_aliases = (
            ("version", "policy_version")
            if any(alias in source for alias in ("version", "policy_version"))
            else ("draft_version",)
        )
        hash_aliases = (
            ("config_hash", "policy_hash")
            if any(alias in source for alias in ("config_hash", "policy_hash"))
            else ("draft_hash",)
        )
        return {
            "policy_id": _rolling_identity_field(
                source, id_aliases, required=False
            ),
            "version": _rolling_identity_field(
                source, version_aliases, required=False
            ),
            "config_hash": _rolling_identity_field(
                source, hash_aliases, required=False
            ),
        }

    outer = fields(value)
    nested_raw = value.get("policy")
    if isinstance(nested_raw, RollingAdmissionPolicy):
        nested: Mapping[str, Any] = nested_raw.as_dict()
    elif isinstance(nested_raw, Mapping):
        nested = nested_raw
    elif nested_raw is None:
        nested = {}
    else:
        raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
    inner = fields(nested)
    for name in ("policy_id", "version", "config_hash"):
        if outer[name] and inner[name] and outer[name] != inner[name]:
            raise OperatorControlError("ROLLING_POLICY_IDENTITY_AMBIGUOUS")
    result = {
        name: outer[name] or inner[name]
        for name in ("policy_id", "version", "config_hash")
    }
    if not result["policy_id"] or not result["version"]:
        raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
    if require_hash and not result["config_hash"]:
        raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
    return result


def _rolling_identity_present(value: Mapping[str, Any]) -> bool:
    if any(alias in value for alias in (
        *_ROLLING_POLICY_ID_ALIASES,
        *_ROLLING_POLICY_VERSION_ALIASES,
        *_ROLLING_POLICY_HASH_ALIASES,
    )):
        return True
    nested = value.get("policy")
    return isinstance(nested, Mapping) and _rolling_identity_present(nested)


def _rolling_review_target(value: Mapping[str, Any]) -> str:
    """Return one canonical review target before action idempotency begins."""
    if "policy" in value and "values" in value:
        raise OperatorControlError("ROLLING_POLICY_INPUT_AMBIGUOUS")
    source_key = "policy" if "policy" in value else "values"
    source = value.get(source_key)
    source = {} if source is None else source
    if not isinstance(source, Mapping):
        raise OperatorControlError("ROLLING_POLICY_REQUIRED")
    if source_key == "policy" or _rolling_identity_present(source):
        identity = _rolling_policy_identity(source)
        return f"{identity['policy_id']}:{identity['version']}"
    return "values:" + hashlib.sha256(
        json.dumps(
            _safe_value(source),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
def _rolling_text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _rolling_decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (ArithmeticError, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _rolling_identity_value(source: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = _rolling_text(source.get(key))
        if value:
            return value
    return ""


def _rolling_actionability(
    member: Mapping[str, Any],
    selection: Mapping[str, Any],
    active_policy: Mapping[str, Any] | None,
    evidence_loader: Callable[..., Any] | None,
) -> tuple[bool, list[str]]:
    """Mirror the rolling worker's funded-member gate and lineage fence."""
    payload = member.get("payload")
    payload = payload if isinstance(payload, Mapping) else {}
    blockers: list[str] = []

    status = _rolling_text(member.get("status") or payload.get("status")).upper()
    if status not in {"ACTIVE", "REDUCE"}:
        blockers.append(f"STATUS_{status or 'MISSING'}")
    allocation = _rolling_decimal(member.get("allocation", payload.get("allocation")))
    if allocation is None:
        blockers.append("ALLOCATION_INVALID")
    elif allocation <= 0:
        blockers.append("ALLOCATION_REQUIRED")

    strategy_id = _rolling_identity_value(member, "strategy_version_id")
    if not strategy_id:
        strategy_id = _rolling_identity_value(payload, "strategy_version_id")
    candidate_id = _rolling_identity_value(member, "candidate_id", "candidate", "strategy_candidate_id")
    if not candidate_id:
        candidate_id = _rolling_identity_value(payload, "candidate_id", "candidate", "strategy_candidate_id")
    trial_id = _rolling_identity_value(member, "research_trial_id", "trial_id")
    if not trial_id:
        trial_id = _rolling_identity_value(payload, "research_trial_id", "trial_id")
    evidence_id = _rolling_identity_value(member, "evidence_window_id")
    if not evidence_id:
        evidence_id = _rolling_identity_value(payload, "evidence_window_id")
    digest = _rolling_identity_value(member, "evidence_digest")
    if not digest:
        digest = _rolling_identity_value(payload, "evidence_digest")
    for value, reason in (
        (strategy_id, "STRATEGY_VERSION_ID_REQUIRED"),
        (candidate_id, "CANDIDATE_ID_REQUIRED"),
        (trial_id, "RESEARCH_TRIAL_ID_REQUIRED"),
        (evidence_id, "EVIDENCE_WINDOW_ID_REQUIRED"),
        (digest, "EVIDENCE_DIGEST_REQUIRED"),
    ):
        if not value:
            blockers.append(reason)

    selection_id = _rolling_identity_value(selection, "portfolio_selection_id", "selection_id")
    member_selection_id = _rolling_identity_value(member, "portfolio_selection_id", "selection_id")
    if not selection_id:
        blockers.append("PORTFOLIO_SELECTION_ID_REQUIRED")
    elif member_selection_id and member_selection_id != selection_id:
        blockers.append("PORTFOLIO_SELECTION_ID_MISMATCH")

    policy_config = selection.get("policy_config")
    policy_config = policy_config if isinstance(policy_config, Mapping) else {}
    policy_id = _rolling_identity_value(selection, "policy_id", "admission_policy_id")
    policy_version = _rolling_identity_value(
        selection,
        "policy_version",
        "version",
        "admission_policy_version",
    )
    policy_hash = _rolling_identity_value(selection, "policy_hash", "config_hash")
    if not policy_hash:
        policy_hash = _rolling_identity_value(policy_config, "config_hash", "policy_hash")
    for value, reason in (
        (policy_id, "POLICY_ID_REQUIRED"),
        (policy_version, "POLICY_VERSION_REQUIRED"),
        (policy_hash, "POLICY_HASH_REQUIRED"),
    ):
        if not value:
            blockers.append(reason)
    if not isinstance(active_policy, Mapping) or not active_policy:
        blockers.append("ACTIVE_POLICY_REQUIRED")
    else:
        try:
            active_identity = _rolling_policy_identity(active_policy)
        except OperatorControlError:
            blockers.append("ACTIVE_POLICY_IDENTITY_INVALID")
        else:
            if (
                policy_id
                and policy_version
                and policy_hash
                and (
                    active_identity["policy_id"] != policy_id
                    or active_identity["version"] != policy_version
                    or active_identity["config_hash"] != policy_hash
                )
            ):
                blockers.append("POLICY_IDENTITY_MISMATCH")

    risk_id = _rolling_identity_value(
        selection,
        "active_risk_config_id",
        "risk_config_id",
    )
    risk_hash = _rolling_identity_value(
        selection,
        "active_risk_config_hash",
        "risk_config_hash",
    )
    risk_generation_raw = selection.get(
        "active_risk_config_generation",
        selection.get("risk_config_generation"),
    )
    risk_generation = _rolling_decimal(risk_generation_raw)
    if not risk_id:
        blockers.append("RISK_CONFIG_ID_REQUIRED")
    if risk_generation is None or risk_generation != risk_generation.to_integral_value() or risk_generation <= 0:
        blockers.append("RISK_CONFIG_GENERATION_REQUIRED")
    if not isinstance(active_policy, Mapping) or not active_policy:
        blockers.append("ACTIVE_RISK_CONFIG_REQUIRED")
    else:
        active_risk_id = _rolling_identity_value(
            active_policy,
            "active_risk_config_id",
            "risk_config_id",
        )
        active_risk_hash = _rolling_identity_value(
            active_policy,
            "active_risk_config_hash",
            "risk_config_hash",
        )
        active_generation = _rolling_decimal(
            active_policy.get(
                "active_risk_config_generation",
                active_policy.get("risk_config_generation"),
            )
        )
        for value, reason in (
            (active_risk_id, "ACTIVE_RISK_CONFIG_ID_REQUIRED"),
            (active_risk_hash, "ACTIVE_RISK_CONFIG_HASH_REQUIRED"),
        ):
            if not value:
                blockers.append(reason)
        if (
            active_generation is None
            or active_generation != active_generation.to_integral_value()
            or active_generation <= 0
        ):
            blockers.append("ACTIVE_RISK_CONFIG_GENERATION_REQUIRED")
        if active_risk_id and risk_id and active_risk_id != risk_id:
            blockers.append("RISK_CONFIG_IDENTITY_MISMATCH")
        if active_risk_hash and risk_hash and active_risk_hash != risk_hash:
            blockers.append("RISK_CONFIG_IDENTITY_MISMATCH")
        if (
            active_generation is not None
            and risk_generation is not None
            and active_generation != risk_generation
        ):
            blockers.append("RISK_CONFIG_IDENTITY_MISMATCH")

    evidence: Mapping[str, Any] | None = None
    nested_evidence = member.get("evidence")
    if not isinstance(nested_evidence, Mapping):
        nested_evidence = payload.get("evidence")
    if isinstance(nested_evidence, Mapping):
        evidence = nested_evidence
    if evidence_loader is not None and strategy_id and evidence_id:
        try:
            rows = evidence_loader(strategy_id, limit=64)
        except TypeError:
            try:
                rows = evidence_loader(strategy_id)
            except TypeError:
                try:
                    rows = evidence_loader(strategy_version_id=strategy_id, limit=64)
                except Exception:
                    rows = ()
            except Exception:
                rows = ()
        except Exception:
            rows = ()
        if isinstance(rows, Mapping):
            rows = (rows,)
        if isinstance(rows, (list, tuple)):
            evidence = next(
                (
                    row
                    for row in rows[:64]
                    if isinstance(row, Mapping)
                    and _rolling_text(row.get("evidence_window_id")) == evidence_id
                ),
                evidence,
            )
    if evidence is None:
        blockers.append("EVIDENCE_WINDOW_UNAVAILABLE")
    else:
        evidence_payload = evidence.get("payload")
        evidence_payload = evidence_payload if isinstance(evidence_payload, Mapping) else {}
        exact_strategy = _rolling_identity_value(evidence, "strategy_version_id") or _rolling_identity_value(
            evidence_payload, "strategy_version_id"
        )
        exact_candidate = _rolling_identity_value(evidence, "candidate_id", "candidate") or _rolling_identity_value(
            evidence_payload, "candidate_id", "candidate"
        )
        exact_trial = _rolling_identity_value(evidence, "research_trial_id", "trial_id") or _rolling_identity_value(
            evidence_payload, "research_trial_id", "trial_id"
        )
        exact_digest = _rolling_identity_value(evidence, "evidence_digest", "digest") or _rolling_identity_value(
            evidence_payload, "evidence_digest", "digest"
        )
        source_class = _rolling_identity_value(evidence, "source_class") or _rolling_identity_value(
            evidence_payload, "source_class"
        )
        if not exact_strategy or exact_strategy != strategy_id:
            blockers.append("EVIDENCE_STRATEGY_MISMATCH")
        if not exact_candidate or exact_candidate != candidate_id:
            blockers.append("EVIDENCE_CANDIDATE_MISMATCH")
        if not exact_trial or exact_trial != trial_id:
            blockers.append("EVIDENCE_TRIAL_MISMATCH")
        if not source_class:
            blockers.append("EVIDENCE_SOURCE_CLASS_REQUIRED")
        if not exact_digest or not digest or exact_digest != digest:
            blockers.append("EVIDENCE_DIGEST_MISMATCH")

    return not blockers, list(dict.fromkeys(blockers))[:32]
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
def _operator_service_identity(service_type: Any) -> str:
    """Project a service class name without assuming class metadata exists."""
    service_class = service_type
    module = getattr(service_class, "__module__", None)
    qualname = getattr(service_class, "__qualname__", None)
    if not isinstance(module, str) or not module.strip():
        module = getattr(type(service_class), "__module__", "") or ""
    if not isinstance(qualname, str) or not qualname.strip():
        qualname = getattr(service_class, "__name__", None)
    if not isinstance(qualname, str) or not qualname.strip():
        qualname = getattr(type(service_class), "__qualname__", None) or "service"
    module_text = str(module).strip()
    qualname_text = str(qualname).strip()
    return f"{module_text}.{qualname_text}" if module_text else qualname_text


_POSITIVE_GENERATION = re.compile(r"^[1-9][0-9]*$")


def _positive_generation(value: Any, code: str) -> int:
    """Accept only an exact positive integer fence, never lossy coercions."""
    if isinstance(value, bool):
        raise OperatorControlError(code)
    if isinstance(value, int):
        generation = value
    elif isinstance(value, str) and _POSITIVE_GENERATION.fullmatch(value):
        generation = int(value)
    else:
        raise OperatorControlError(code)
    if generation < 1:
        raise OperatorControlError(code)
    return generation


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
    """Allowlisted, localhost-only operator action control plane."""

    def __init__(
        self,
        store: AxiomStore,
        *,
        db_path: str | None = None,
        node_launcher: Callable[[list[str]], subprocess.Popen[Any]] | None = None,
        settings_service: CanarySettingsService | None = None,
        hermes_job_id: str | None = None,
        execution_profile: str | None = None,
        profile: Any | None = None,
        historical_refresh_enabled: bool = False,
        historical_refresh_interval_seconds: float = 3600.0,
        historical_refresh_request_budget: int = 25,
        historical_refresh_market_budget: int = 4,
    ) -> None:
        self.store = store
        ambient_profile = os.environ.get(EXECUTION_PROFILE_ENV)
        selected_profile = (
            profile
            if profile is not None
            else (
                ambient_profile
                if ambient_profile not in {None, ""}
                else execution_profile
            )
        )
        try:
            self.execution_profile = normalized_execution_profile(
                selected_profile,
                default=PRODUCTION_EXECUTION_PROFILE,
            )
        except (TypeError, ValueError):
            # An invalid ambient profile is an isolation fence, never a
            # production fallback.  Node start/profile checks still report it
            # as invalid rather than claiming a canonical profile.
            self.execution_profile = None
        self.settings = settings_service or CanarySettingsService(store)
        raw_db = db_path if db_path is not None else getattr(store, "path", "")
        self.db_path = os.path.abspath(os.path.expanduser(str(raw_db))) if str(raw_db) not in {"", ":memory:"} else str(raw_db)
        self._node_launcher = node_launcher or self._spawn_node
        self.historical_refresh_enabled = bool(historical_refresh_enabled)
        self.historical_refresh_interval_seconds = float(historical_refresh_interval_seconds)
        self.historical_refresh_request_budget = int(historical_refresh_request_budget)
        self.historical_refresh_market_budget = int(historical_refresh_market_budget)
        self._lock = threading.RLock()
        self._connectivity_lock = threading.RLock()
        self._action_lock = threading.RLock()
        self._bootstrap_threads: dict[str, threading.Thread] = {}
        # Windows identity checks can invoke CIM and are disproportionately
        # expensive on a dashboard refresh.  Keep a tiny, short-lived cache so
        # repeated status reads cannot fan out into an unbounded process scan.
        self._pid_identity_cache: dict[tuple[int, str, int], tuple[float, bool]] = {}
        self._pid_identity_cache_ttl = 1.0
        self._pid_identity_cache_limit = 32
        configured = self.store.get_operator_config("hermes_research_job_id", None)
        selected = hermes_job_id or configured or DEFAULT_HERMES_JOB_ID
        self.hermes_job_id = _safe_identifier(selected, "Hermes job ID")
        if hermes_job_id is not None:
            self.configure_hermes_job_id(hermes_job_id)
        # Construct read services once during startup. Their constructors may
        # establish missing schema/defaults, but GET/status paths must never
        # construct them again.
        self._canary_service = CanaryService(
            self.store,
            initialize=False,
            settings=self.settings,
        )
        self._research_processor = AutonomousResearchProcessor(self.store, clock=utc_now)
    def risk_settings_snapshot(self) -> dict[str, Any]:
        """Return the bounded persisted active/draft risk settings projection."""
        snapshot = self.settings.snapshot()
        projected = _safe_value(snapshot)
        if not isinstance(projected, Mapping):
            raise OperatorControlError("RISK_SETTINGS_UNAVAILABLE")
        return dict(projected)

    def save_risk_settings_draft(
        self,
        values: Mapping[str, Any],
        *,
        actor: str = "operator",
    ) -> dict[str, Any]:
        if not isinstance(values, Mapping):
            raise OperatorControlError("RISK_SETTINGS_VALUES_REQUIRED")
        actor_value = _safe_identifier(actor, "actor")
        result = self.settings.save_draft(dict(values), actor_value)
        projected = _safe_value(result)
        if not isinstance(projected, Mapping):
            raise OperatorControlError("RISK_SETTINGS_SAVE_FAILED")
        return dict(projected)

    def activate_risk_settings_draft(
        self,
        config_id: Any,
        *,
        actor: str = "operator",
        expected_generation: Any = None,
    ) -> dict[str, Any]:
        if not isinstance(config_id, str):
            raise OperatorControlError("RISK_SETTINGS_CONFIG_REQUIRED")
        config_value = _safe_identifier(config_id, "risk settings config ID")
        actor_value = _safe_identifier(actor, "actor")
        generation = _positive_generation(
            expected_generation,
            "RISK_SETTINGS_GENERATION_REQUIRED",
        )
        result = self.settings.activate_draft(config_value, actor_value, generation)
        projected = _safe_value(result)
        if not isinstance(projected, Mapping):
            raise OperatorControlError("RISK_SETTINGS_ACTIVATE_FAILED")
        return dict(projected)
    @staticmethod
    def _authorization_text(value: Any, field: str, *, limit: int = 256) -> str:
        if not isinstance(value, str):
            raise OperatorControlError(f"EXECUTION_AUTHORIZATION_{field.upper()}_REQUIRED")
        text = value.strip()
        if not text or len(text) > limit or _SECRET_KEY.search(text):
            raise OperatorControlError(f"EXECUTION_AUTHORIZATION_{field.upper()}_INVALID")
        return text

    @staticmethod
    def _authorization_decimal(value: Any, field: str) -> str:
        if isinstance(value, bool) or value is None:
            raise OperatorControlError(f"EXECUTION_AUTHORIZATION_{field.upper()}_REQUIRED")
        try:
            decimal_value = Decimal(str(value).strip())
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise OperatorControlError(
                f"EXECUTION_AUTHORIZATION_{field.upper()}_INVALID"
            ) from exc
        if not decimal_value.is_finite() or decimal_value <= 0:
            raise OperatorControlError(f"EXECUTION_AUTHORIZATION_{field.upper()}_INVALID")
        return format(decimal_value, "f")

    @staticmethod
    def _authorization_timestamp(value: Any, field: str) -> str:
        if isinstance(value, datetime):
            stamp = value
        elif isinstance(value, str):
            try:
                stamp = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            except ValueError as exc:
                raise OperatorControlError(
                    f"EXECUTION_AUTHORIZATION_{field.upper()}_INVALID"
                ) from exc
        else:
            raise OperatorControlError(f"EXECUTION_AUTHORIZATION_{field.upper()}_REQUIRED")
        if stamp.tzinfo is None:
            raise OperatorControlError(f"EXECUTION_AUTHORIZATION_{field.upper()}_INVALID")
        return stamp.astimezone(timezone.utc).isoformat()

    def _authorization_context(self) -> dict[str, Any]:
        """Derive immutable authorization bindings from persisted state.

        The browser supplies intent and bounded stop values only.  Strategy,
        selection, scope, and settings identities are read from the same
        database that the execution service consumes.
        """
        now = utc_now()
        risk = self.risk_settings_snapshot()
        active_settings = risk.get("active") if isinstance(risk, Mapping) else {}
        active_settings = active_settings if isinstance(active_settings, Mapping) else {}
        settings_hash = str(
            risk.get("config_hash") or active_settings.get("config_hash") or ""
        ).strip()
        settings_generation = risk.get("generation") or active_settings.get("generation")
        try:
            settings_generation = int(settings_generation)
        except (TypeError, ValueError, OverflowError):
            settings_generation = 0
        if not settings_hash or settings_generation <= 0:
            raise OperatorControlError("ACTIVE_RISK_CONFIG_REQUIRED")

        selection_loader = getattr(self.store, "load_current_portfolio_selection", None)
        try:
            selection_raw = selection_loader() if callable(selection_loader) else None
        except Exception as exc:
            raise OperatorControlError(
                "EXECUTION_AUTHORIZATION_SELECTION_UNAVAILABLE",
                type(exc).__name__,
            ) from exc
        selection = dict(selection_raw) if isinstance(selection_raw, Mapping) else {}
        selection_id = str(
            selection.get("portfolio_selection_id") or selection.get("selection_id") or ""
        ).strip()
        try:
            selection_hash = _canary_authorization_current_selection_hash(selection)
        except CanaryBlocked as exc:
            raise OperatorControlError(str(exc) or "EXECUTION_AUTHORIZATION_SELECTION_INVALID") from exc
        members = selection.get("members", selection.get("selected_members", ()))
        members = members if isinstance(members, (list, tuple)) else ()
        strategy_versions: list[str] = []
        rejected_strategy_versions: list[str] = []
        candidate_loader = getattr(self.store, "load_candidate_lifecycle", None)
        for member in members[:64]:
            if not isinstance(member, Mapping):
                continue
            status = str(
                member.get("status")
                or member.get("stage")
                or ""
            ).strip().upper()
            rejected = bool(member.get("rejected")) or status == "REJECTED"
            try:
                allocation = Decimal(str(member.get("allocation") or "0"))
            except (ArithmeticError, TypeError, ValueError):
                allocation = Decimal("0")
            # Keep rejected identities in the review binding even when their
            # allocator has already set their opening allocation to zero.
            if (
                not allocation.is_finite()
                or allocation <= 0
            ) and not rejected:
                continue
            identifier = str(member.get("strategy_version_id") or "").strip()
            if not identifier:
                continue
            identifier = identifier[:256]
            if identifier not in strategy_versions:
                strategy_versions.append(identifier)
            candidate_id = str(member.get("candidate_id") or "").strip()
            if not rejected and callable(candidate_loader) and candidate_id:
                try:
                    candidate = candidate_loader(candidate_id)
                except Exception:
                    candidate = None
                if isinstance(candidate, Mapping):
                    candidate_payload = candidate.get("payload")
                    candidate_payload = (
                        candidate_payload
                        if isinstance(candidate_payload, Mapping)
                        else {}
                    )
                    rejected = (
                        str(candidate.get("stage") or "").strip().upper()
                        == "REJECTED"
                        or str(candidate_payload.get("stage") or "").strip().upper()
                        == "REJECTED"
                        or bool(candidate_payload.get("rejected"))
                    )
            if rejected and identifier not in rejected_strategy_versions:
                rejected_strategy_versions.append(identifier)
        try:
            rolling = self.rolling_portfolio_state()
        except Exception as exc:
            raise OperatorControlError(
                "EXECUTION_AUTHORIZATION_POLICY_UNAVAILABLE",
                type(exc).__name__,
            ) from exc
        policy_identity = rolling.get("active_policy_identity")
        if not isinstance(policy_identity, Mapping):
            policy_identity = rolling.get("policy_identity")
        policy_identity = dict(policy_identity) if isinstance(policy_identity, Mapping) else {}
        policy_hash = (
            str(
                policy_identity.get("config_hash")
                or policy_identity.get("policy_hash")
                or selection.get("policy_hash")
                or selection.get("config_hash")
                or ""
            ).strip()
        )
        policy_id = str(
            policy_identity.get("policy_id")
            or policy_identity.get("id")
            or selection.get("policy_id")
            or selection.get("admission_policy_id")
            or ""
        ).strip() or None
        policy_version = str(
            policy_identity.get("policy_version")
            or policy_identity.get("version")
            or selection.get("policy_version")
            or selection.get("admission_policy_version")
            or ""
        ).strip() or None
        setup_bindings: list[dict[str, Any]] = []
        for member in members[:64]:
            if not isinstance(member, Mapping):
                continue
            setup = member.get("operational_setup")
            setup = setup if isinstance(setup, Mapping) else {}
            strategy_id = str(member.get("strategy_version_id") or "").strip()
            if not strategy_id:
                continue
            setup_bindings.append(
                {
                    "strategy_version_id": strategy_id,
                    "candidate_id": str(member.get("candidate_id") or "").strip() or None,
                    "setup_id": member.get("setup_id", setup.get("setup_id")),
                    "setup_version": member.get("setup_version", setup.get("setup_version")),
                    "setup_hash": member.get(
                        "operational_setup_hash",
                        setup.get("operational_setup_hash", setup.get("setup_hash")),
                    ),
                    "scope_hash": member.get("scope_hash", setup.get("scope_hash")),
                    "scope_version": member.get("scope_version", setup.get("scope_version")),
                }
            )
        scope_loader = getattr(self.store, "market_scope_resolution_funnel", None)
        if callable(scope_loader):
            try:
                scope = scope_loader(limit=1000)
            except TypeError:
                try:
                    scope = scope_loader()
                except Exception as exc:
                    raise OperatorControlError(
                        "EXECUTION_AUTHORIZATION_SCOPE_UNAVAILABLE",
                        type(exc).__name__,
                    ) from exc
            except Exception as exc:
                raise OperatorControlError(
                    "EXECUTION_AUTHORIZATION_SCOPE_UNAVAILABLE",
                    type(exc).__name__,
                ) from exc
        scope = scope if isinstance(scope, Mapping) else {}
        scope_hash = self._rolling_canonical_hash(scope)
        scope_version = str(
            scope.get("scope_version")
            or scope.get("version")
            or scope.get("snapshot_version")
            or "scope-v1"
        ).strip()[:128]
        limits = risk.get("effective_limits", risk.get("active_limits", {}))
        limits = dict(limits) if isinstance(limits, Mapping) else {}
        return {
            "now": now,
            "selection_id": selection_id or None,
            "selection_hash": selection_hash or None,
            "selection_policy_hash": policy_hash or None,
            "strategy_versions": strategy_versions,
            "rejected_strategy_versions": rejected_strategy_versions,
            "policy_id": policy_id,
            "policy_version": policy_version,
            "policy_hash": policy_hash or None,
            "setup_bindings": setup_bindings,
            "scope_hash": scope_hash,
            "scope_version": scope_version,
            "active_settings_hash": settings_hash,
            "active_settings_generation": settings_generation,
            "proposed_allocation_total": selection.get("proposed_allocation_total"),
            "proposed_allocation_risk_digest": selection.get(
                "proposed_allocation_risk_digest"
            ),
            "limits": limits,
            "selection": selection,
            "scope": scope,
        }
    def execution_authorization_snapshot(self) -> dict[str, Any]:
        """Return active and latest reviewed exploratory authorization state."""
        loader = getattr(self.store, "load_active_execution_authorization", None)
        active: Mapping[str, Any] | None = None
        if callable(loader):
            try:
                value = loader(mode="EXPLORATORY_MICRO_CANARY", now=utc_now())
            except TypeError:
                value = loader()
            except Exception:
                value = None
            if isinstance(value, Mapping):
                active = value

        # Keep the latest durable record visible even when the active loader
        # deliberately hides an expired or stale-binding authorization.  A
        # dashboard must not turn those states into an indistinguishable
        # disabled/active projection.
        latest: Mapping[str, Any] | None = None
        list_authorizations = getattr(
            self.store, "list_execution_authorizations", None
        )
        if callable(list_authorizations):
            try:
                records = list_authorizations(
                    mode="EXPLORATORY_MICRO_CANARY",
                    limit=1,
                    now=utc_now(),
                )
            except TypeError:
                records = list_authorizations(
                    mode="EXPLORATORY_MICRO_CANARY",
                    limit=1,
                )
            except Exception:
                records = ()
            if isinstance(records, (list, tuple)) and records:
                candidate = records[0]
                if isinstance(candidate, Mapping):
                    latest = candidate
                    if active is None and str(
                        candidate.get("status") or ""
                    ).upper() == "ACTIVE":
                        active = candidate

        get_config = getattr(self.store, "get_operator_config", None)
        try:
            draft = (
                get_config("execution_authorization_review", None)
                if callable(get_config)
                else None
            )
        except Exception:
            draft = None
        draft = dict(draft) if isinstance(draft, Mapping) else None

        latest_status = (
            str(latest.get("status") or "").strip().upper()
            if isinstance(latest, Mapping)
            else ""
        )
        if (
            latest is not None
            and latest_status in {"DRAFT", "EXPIRED", "REVOKED"}
            and (
                draft is None
                or str(draft.get("status") or "").strip().upper() == "ACTIVE"
            )
        ):
            # The operator config is a compatibility cache and can still
            # carry ACTIVE after a restart.  Prefer the durable row's
            # terminal state and generated identifier.
            draft = dict(latest)

        draft_status = (
            str(draft.get("status") or "").strip().upper()
            if isinstance(draft, Mapping)
            else ""
        )
        if isinstance(active, Mapping):
            status = str(active.get("status") or "ACTIVE").upper()
        elif latest_status in {"DRAFT", "EXPIRED", "REVOKED", "UNKNOWN"}:
            status = latest_status
        elif draft_status in {"DRAFT", "EXPIRED", "REVOKED", "UNKNOWN"}:
            status = draft_status
        elif draft is not None and "status" in draft:
            status = "UNKNOWN"
        else:
            status = "DISABLED"
        if isinstance(active, Mapping) and isinstance(draft, Mapping):
            active_id = str(active.get("authorization_id") or active.get("id") or "").strip()
            draft_id = str(draft.get("authorization_id") or draft.get("id") or "").strip()
            if active_id and active_id == draft_id:
                enriched = dict(active)
                for key in (
                    "selection_id",
                    "selection_hash",
                    "policy_id",
                    "policy_version",
                    "policy_hash",
                    "setup_bindings",
                ):
                    if enriched.get(key) in (None, "", [], {}):
                        enriched[key] = draft.get(key)
                active = enriched
        authorization = active or latest or draft
        authorization_id = (
            authorization.get("authorization_id") or authorization.get("id")
            if isinstance(authorization, Mapping)
            else None
        )
        authorization_mode = (
            str(authorization.get("mode") or "").strip().upper()
            if isinstance(authorization, Mapping)
            else ""
        ) or "EXPLORATORY_MICRO_CANARY"
        return {
            "active": _safe_value(active) if active is not None else None,
            "draft": _safe_value(draft) if draft is not None else None,
            "authorization": _safe_value(authorization)
            if authorization is not None
            else None,
            "authorization_id": str(authorization_id).strip()
            if authorization_id
            else None,
            "status": status,
            "mode": authorization_mode,
            "paper_only": True,
            "live_execution": False,
        }

    def review_execution_authorization(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        actor: str = "operator",
    ) -> dict[str, Any]:
        """Persist a bounded exploratory authorization draft without activating it."""
        if values is not None and not isinstance(values, Mapping):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_VALUES_REQUIRED")
        raw = dict(values or {})
        context = self._authorization_context()
        allowed = {
            "purpose",
            "exact_strategy_versions",
            "strategy_version_ids",
            "reviewed_selection_policy_hash",
            "selection_policy_hash",
            "policy_hash",
            "adverse_evidence_ack",
            "lifetime_budget",
            "stop_rules",
            "expires_at",
            "scope_hash",
            "scope_version",
            "active_settings_hash",
            "active_settings_generation",
            "selection_id",
            "selection_hash",
            "proposed_allocation_total",
            "proposed_allocation_risk_digest",
            "actor_version",
        }
        unknown = set(raw) - allowed
        if unknown:
            raise OperatorControlError("UNSUPPORTED_EXECUTION_AUTHORIZATION_FIELDS")
        for field, context_key in (
            ("scope_hash", "scope_hash"),
            ("scope_version", "scope_version"),
            ("active_settings_hash", "active_settings_hash"),
            ("proposed_allocation_total", "proposed_allocation_total"),
            ("proposed_allocation_risk_digest", "proposed_allocation_risk_digest"),
        ):
            if field not in raw:
                continue
            supplied = raw.get(field)
            expected = context.get(context_key)
            if str(supplied or "") != str(expected or ""):
                raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
        for field, context_key in (
            ("active_settings_generation", "active_settings_generation"),
            ("selection_id", "selection_id"),
            ("selection_hash", "selection_hash"),
        ):
            if field not in raw:
                continue
            supplied = raw.get(field)
            expected = context.get(context_key)
            if field == "active_settings_generation":
                if isinstance(supplied, bool) or isinstance(expected, bool):
                    raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
                try:
                    if int(supplied) != int(expected):
                        raise OperatorControlError(
                            "EXECUTION_AUTHORIZATION_BINDING_STALE"
                        )
                except (TypeError, ValueError, OverflowError):
                    raise OperatorControlError(
                        "EXECUTION_AUTHORIZATION_BINDING_STALE"
                    ) from None
            elif str(supplied or "") != str(expected or ""):
                raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
        purpose = self._authorization_text(
            raw.get("purpose", "exploratory micro-canary review"), "purpose"
        )
        actor_value = _safe_identifier(actor, "actor")
        actor_version = self._authorization_text(
            raw.get("actor_version", os.environ.get("AXIOM_OPERATOR_VERSION", "operator-v1")),
            "actor_version",
            limit=64,
        )
        strategy_values = raw.get("exact_strategy_versions", raw.get("strategy_version_ids"))
        if strategy_values is None:
            strategy_values = context["strategy_versions"]
        if isinstance(strategy_values, str):
            strategy_values = (strategy_values,)
        if not isinstance(strategy_values, (list, tuple, set, frozenset)):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_STRATEGIES_INVALID")
        strategy_versions: list[str] = []
        for item in strategy_values:
            value = self._authorization_text(item, "strategy_version", limit=256)
            if value not in strategy_versions:
                strategy_versions.append(value)
            if len(strategy_versions) >= 32:
                break
        if "exact_strategy_versions" in raw or "strategy_version_ids" in raw:
            if not set(strategy_versions).issubset(set(context["strategy_versions"])):
                raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
        ack = raw.get("adverse_evidence_ack")
        rejected_strategy_versions = {
            str(value).strip()
            for value in context.get("rejected_strategy_versions", ())
            if str(value).strip()
        }
        policy_values = {
            str(raw[name]).strip()
            for name in (
                "reviewed_selection_policy_hash",
                "selection_policy_hash",
                "policy_hash",
            )
            if raw.get(name) not in (None, "")
            and str(raw.get(name)).strip()
        }
        if len(policy_values) > 1:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
        supplied_policy_hash = next(iter(policy_values), "")
        context_policy_hash = str(context.get("selection_policy_hash") or "").strip()
        if supplied_policy_hash and supplied_policy_hash != context_policy_hash:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
        policy_hash = supplied_policy_hash or context_policy_hash
        policy_only = not strategy_versions and bool(policy_hash)
        funding_rejected_strategy = bool(
            rejected_strategy_versions.intersection(strategy_versions)
        )
        if policy_only:
            funding_rejected_strategy = bool(rejected_strategy_versions)
        if ack is True:
            # Browser checkboxes submit a bare boolean.  Convert that
            # shorthand before the strict rejected-member fence runs so a
            # positive operator action carries the required durable marker.
            ack = {
                "acknowledged": True,
                "required": funding_rejected_strategy,
            }
        acknowledged = _canary_adverse_acknowledged(ack)
        rejected_acknowledged = _canary_adverse_acknowledged(
            ack,
            require_required=True,
        )
        if funding_rejected_strategy and not rejected_acknowledged:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_ADVERSE_EVIDENCE_ACK_REQUIRED")
        if (
            policy_only
            and (
                not context.get("selection_id")
                or not context.get("selection_hash")
            )
        ):
            raise OperatorControlError(
                "EXECUTION_AUTHORIZATION_SELECTION_BINDING_REQUIRED"
            )
        # The storage contract requires an explicit acknowledgment object for
        # every durable row.  For accepted strategies this is an internal
        # non-required marker; rejected strategies must carry the operator's
        # explicit acknowledgment above.
        persisted_ack = ack if acknowledged else {
            "acknowledged": True,
            "required": False,
        }
        if not strategy_versions and not policy_hash:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_SELECTION_BINDING_REQUIRED")
        lifetime_budget = self._authorization_decimal(
            raw.get("lifetime_budget", "1.00"), "lifetime_budget"
        )
        now = context["now"]
        expires_value = raw.get("expires_at")
        if expires_value is None:
            expires_at = now.replace(microsecond=0) + timedelta(hours=24)
        else:
            expires_text = self._authorization_timestamp(expires_value, "expires_at")
            try:
                expires_at = datetime.fromisoformat(expires_text)
            except (TypeError, ValueError):
                raise OperatorControlError(
                    "EXECUTION_AUTHORIZATION_EXPIRES_AT_INVALID"
                ) from None
        if expires_at <= now:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_EXPIRED")
        expires_text = expires_at.isoformat()
        stop_rules = raw.get(
            "stop_rules",
            {
                "max_submissions": 1,
                "max_loss_usd": "0.25",
                "halt_on_unknown_execution": True,
            },
        )
        if not isinstance(stop_rules, Mapping) or not stop_rules:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_STOP_RULES_REQUIRED")
        stop_rules = _safe_value(dict(stop_rules))
        if not isinstance(stop_rules, Mapping):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_STOP_RULES_INVALID")
        register = getattr(self.store, "register_execution_authorization_draft", None)
        if not callable(register):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_STORAGE_UNAVAILABLE")
        try:
            draft = register(
                authorization_id=None,
                mode="EXPLORATORY_MICRO_CANARY",
                purpose=purpose,
                exact_strategy_versions=tuple(strategy_versions),
                strategy_version_ids=tuple(strategy_versions),
                reviewed_selection_policy_hash=policy_hash or None,
                selection_policy_hash=policy_hash or None,
                adverse_evidence_ack=persisted_ack,
                lifetime_budget=lifetime_budget,
                stop_rules=dict(stop_rules),
                expires_at=expires_at,
                scope_hash=str(raw.get("scope_hash") or context["scope_hash"]),
                scope_version=str(raw.get("scope_version") or context["scope_version"]),
                active_settings_hash=str(
                    raw.get("active_settings_hash") or context["active_settings_hash"]
                ),
                active_settings_generation=int(
                    raw.get("active_settings_generation")
                    or context["active_settings_generation"]
                ),
                selection_id=str(raw.get("selection_id") or context["selection_id"] or "") or None,
                selection_hash=str(raw.get("selection_hash") or context["selection_hash"] or "") or None,
                actor=actor_value,
                actor_version=actor_version,
                timestamp=now,
            )
        except OperatorControlError:
            raise
        except Exception as exc:
            raise OperatorControlError(
                "EXECUTION_AUTHORIZATION_DRAFT_FAILED", type(exc).__name__
            ) from exc
        projected = _safe_value(draft)
        if not isinstance(projected, Mapping):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_DRAFT_INVALID")
        document = dict(projected)
        document.update(
            {
                "mode": "EXPLORATORY_MICRO_CANARY",
                "purpose": purpose,
                "strategy_versions": strategy_versions,
                "exact_strategy_versions": strategy_versions,
                "adverse_evidence_ack": persisted_ack,
                "adverse_evidence_ack_required": funding_rejected_strategy,
                "lifetime_budget": lifetime_budget,
                "stop_rules": dict(stop_rules),
                "expires_at": expires_text,
                "scope_hash": context["scope_hash"],
                "scope_version": context["scope_version"],
                "active_settings_hash": context["active_settings_hash"],
                "active_settings_generation": context["active_settings_generation"],
                "selection_id": context["selection_id"],
                "selection_hash": context["selection_hash"],
                "policy_id": context.get("policy_id"),
                "policy_version": context.get("policy_version"),
                "policy_hash": context.get("policy_hash"),
                "setup_bindings": _safe_value(context.get("setup_bindings", [])),
                "actor": actor_value,
                "actor_version": actor_version,
                "status": "DRAFT",
                "paper_only": True,
                "proposed_allocation_total": context.get(
                    "proposed_allocation_total"
                ),
                "proposed_allocation_risk_digest": context.get(
                    "proposed_allocation_risk_digest"
                ),
                "live_execution": False,
            }
        )
        self.store.set_operator_config("execution_authorization_review", document)
        return {
            "status": "DRAFT",
            "draft": document,
            "authorization": document,
            "paper_only": True,
            "live_execution": False,
        }

    def activate_execution_authorization(
        self,
        authorization_id: Any | None = None,
        *,
        actor: str = "operator",
        expected_generation: Any | None = None,
    ) -> dict[str, Any]:
        """Activate only the reviewed immutable exploratory record."""
        get_config = getattr(self.store, "get_operator_config", None)
        draft = get_config("execution_authorization_review", None) if callable(get_config) else None
        draft = dict(draft) if isinstance(draft, Mapping) else {}
        if str(draft.get("status") or "").upper() != "DRAFT":
            raise OperatorControlError("EXECUTION_AUTHORIZATION_DRAFT_REQUIRED")
        draft_id = str(draft.get("authorization_id") or draft.get("id") or "").strip()
        if not draft_id:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_DRAFT_REQUIRED")
        if authorization_id is not None and str(authorization_id).strip() != draft_id:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
        identifier = draft_id
        actor_value = _safe_identifier(actor, "actor")
        generation = expected_generation
        if generation is None:
            generation = draft.get("generation")
        if generation is not None:
            try:
                generation = _positive_generation(generation, "EXECUTION_AUTHORIZATION_GENERATION_REQUIRED")
            except OperatorControlError:
                raise
        activate = getattr(self.store, "activate_execution_authorization", None)
        if not callable(activate):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_STORAGE_UNAVAILABLE")
        try:
            result = activate(
                identifier,
                actor_value,
                expected_generation=generation,
                timestamp=utc_now(),
            )
        except OperatorControlError:
            raise
        except Exception as exc:
            raise OperatorControlError(
                "EXECUTION_AUTHORIZATION_ACTIVATE_FAILED", type(exc).__name__
            ) from exc
        projected = _safe_value(result)
        if not isinstance(projected, Mapping):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_ACTIVE_INVALID")
        document = dict(projected)
        document.setdefault(
            "strategy_versions",
            document.get("exact_strategy_versions", document.get("strategy_version_ids", [])),
        )
        document.setdefault("mode", "EXPLORATORY_MICRO_CANARY")
        document["paper_only"] = True
        document["live_execution"] = False
        self.store.set_operator_config("execution_authorization_review", document)
        return {
            "status": str(document.get("status") or "ACTIVE").upper(),
            "authorization": document,
            "paper_only": True,
            "live_execution": False,
        }

    def revoke_execution_authorization(
        self,
        authorization_id: Any | None = None,
        *,
        actor: str = "operator",
        expected_generation: Any | None = None,
        reason: str = "operator_revoke",
    ) -> dict[str, Any]:
        """Revoke the active exploratory record; never changes paper state."""
        active = self.execution_authorization_snapshot().get("active")
        active = active if isinstance(active, Mapping) else {}
        active_id = str(
            active.get("authorization_id") or active.get("id") or ""
        ).strip()
        if not active_id:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_ACTIVE_REQUIRED")
        if authorization_id is not None and str(authorization_id).strip() != active_id:
            raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
        identifier = active_id
        generation = expected_generation
        if generation is None:
            generation = active.get("generation")
        if generation is not None:
            generation = _positive_generation(
                generation, "EXECUTION_AUTHORIZATION_GENERATION_REQUIRED"
            )
        revoke = getattr(self.store, "revoke_execution_authorization", None)
        if not callable(revoke):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_STORAGE_UNAVAILABLE")
        try:
            result = revoke(
                identifier,
                _safe_identifier(actor, "actor"),
                expected_generation=generation,
                reason=self._authorization_text(reason, "revoke_reason", limit=128),
                timestamp=utc_now(),
            )
        except OperatorControlError:
            raise
        except Exception as exc:
            raise OperatorControlError(
                "EXECUTION_AUTHORIZATION_REVOKE_FAILED", type(exc).__name__
            ) from exc
        projected = _safe_value(result)
        if not isinstance(projected, Mapping):
            raise OperatorControlError("EXECUTION_AUTHORIZATION_REVOKED_INVALID")
        document = dict(projected)
        document["paper_only"] = True
        document["live_execution"] = False
        self.store.set_operator_config("execution_authorization_review", document)
        return {
            "status": str(document.get("status") or "REVOKED").upper(),
            "authorization": document,
            "paper_only": True,
            "live_execution": False,
        }

    _EXPLORATORY_LIVE_REVIEW_LIMITS = {
        "max_all_in_buy_usd": "1.00",
        "max_fee_reserve_usd": "0.01",
        "max_gross_daily_buy_usd": "5.00",
        "max_aggregate_open_cost_usd": "5.00",
        "max_aggregate_exposure_usd": "5.00",
        "max_positions": 3,
        "max_submitted_orders_per_day": 5,
        "realized_loss_entry_stop_usd": "2.00",
        "equity_loss_entry_stop_usd": "2.00",
        "max_slippage_bps": 100,
    }
    @staticmethod
    def _exploratory_live_policy_mode(value: Any) -> str:
        if isinstance(value, Mapping):
            value = value.get("mode") or value.get("name") or value.get("type")
        return str(value or "").strip().upper()

    def _selected_market_readiness(
        self,
        selection: Mapping[str, Any],
        *,
        scope: Mapping[str, Any] | None = None,
        target_candidate_id: str | None = None,
    ) -> dict[str, Any]:
        """Require fresh account and selected-market readiness evidence."""
        getter = getattr(self.store, "get_operator_config", None)
        connectivity = getter(CANARY_CONNECTIVITY_CONFIG_KEY, {}) if callable(getter) else {}
        connectivity = dict(connectivity) if isinstance(connectivity, Mapping) else {}
        diagnostics = connectivity.get("diagnostics")
        diagnostics = dict(diagnostics) if isinstance(diagnostics, Mapping) else {}
        checked_at = connectivity.get("checked_at")
        fresh = False
        if isinstance(checked_at, str):
            try:
                stamp = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
                age = (utc_now() - stamp.astimezone(timezone.utc)).total_seconds()
                fresh = 0 <= age <= CANARY_READINESS_SNAPSHOT_MAX_AGE_SECONDS
            except (TypeError, ValueError):
                fresh = False
        members = selection.get("members", selection.get("selected_members", ()))
        if isinstance(members, (list, tuple)) and target_candidate_id:
            first_member = next(
                (
                    item
                    for item in members
                    if isinstance(item, Mapping)
                    and str(item.get("candidate_id") or "").strip()
                    == str(target_candidate_id).strip()
                ),
                {},
            )
        else:
            first_member = next(
                (item for item in members if isinstance(item, Mapping))
                if isinstance(members, (list, tuple))
                else iter(())
            )
        first_member = first_member if isinstance(first_member, Mapping) else {}
        canonical_scope = first_member.get("market_scope")
        canonical_scope = canonical_scope if isinstance(canonical_scope, Mapping) else {}
        exact_market_ids = canonical_scope.get(
            "market_ids", canonical_scope.get("exact_market_ids", ())
        )
        if isinstance(exact_market_ids, str):
            exact_market_ids = (exact_market_ids,)
        exact_market_ids = tuple(
            str(value).strip()
            for value in (exact_market_ids or ())
            if str(value).strip()
        )
        materialized: list[Mapping[str, Any]] = []
        candidate_id = str(first_member.get("candidate_id") or "").strip()
        resolution_loader = getattr(self.store, "load_current_market_resolution", None)
        if not callable(resolution_loader):
            resolution_loader = getattr(self.store, "load_market_scope_resolution", None)
        if callable(resolution_loader) and candidate_id:
            try:
                resolution = resolution_loader(
                    candidate_id,
                    scope_hash=str(first_member.get("scope_hash") or "").strip() or None,
                    scope_version=str(first_member.get("scope_version") or "").strip() or None,
                )
            except TypeError:
                try:
                    resolution = resolution_loader(candidate_id)
                except Exception:
                    resolution = None
            except Exception:
                resolution = None
            matched = getattr(resolution, "matched_markets", None)
            if matched is None and isinstance(resolution, Mapping):
                matched = resolution.get("matched_markets", resolution.get("markets", ()))
            if isinstance(matched, (list, tuple)):
                for item in matched:
                    if hasattr(item, "as_dict") and callable(item.as_dict):
                        item = item.as_dict()
                    if isinstance(item, Mapping):
                        materialized.append(dict(item))
        if isinstance(scope, Mapping):
            for key in ("current_markets", "matched_markets", "markets"):
                values = scope.get(key)
                if isinstance(values, (list, tuple)):
                    materialized.extend(
                        dict(item) for item in values if isinstance(item, Mapping)
                    )
        market_binding = (
            first_member.get("current_market_binding")
            or first_member.get("market_binding")
            or first_member.get("selected_market")
        )
        market_binding = market_binding if isinstance(market_binding, Mapping) else {}
        if market_binding:
            materialized.append(dict(market_binding))
        binding_market_id = str(
            market_binding.get("market_id")
            or market_binding.get("id")
            or market_binding.get("market")
            or ""
        ).strip()
        candidates = []
        for item in materialized:
            item_market = str(
                item.get("market_id") or item.get("id") or item.get("market") or ""
            ).strip()
            if (
                item_market
                and (not exact_market_ids or item_market in exact_market_ids)
                and (not binding_market_id or item_market == binding_market_id)
            ):
                candidates.append(item)
        unique_candidates: dict[str, dict[str, Any]] = {}
        for item in candidates:
            item_market = str(
                item.get("market_id") or item.get("id") or item.get("market") or ""
            ).strip()
            existing = unique_candidates.get(item_market)
            if existing is None:
                unique_candidates[item_market] = dict(item)
            else:
                for key, value in item.items():
                    if existing.get(key) in (None, "", {}, []):
                        existing[key] = value
        candidates = list(unique_candidates.values())
        requested_market = ""
        requested_token = ""
        selected_materialized: Mapping[str, Any] = {}
        if len(candidates) == 1:
            selected_materialized = candidates[0]
            requested_market = str(
                selected_materialized.get("market_id")
                or selected_materialized.get("id")
                or selected_materialized.get("market")
                or ""
            ).strip()
            setup = first_member.get("operational_setup")
            setup = setup if isinstance(setup, Mapping) else {}
            capture_spec = setup.get("capture_spec")
            capture_spec = capture_spec if isinstance(capture_spec, Mapping) else {}
            outcome_mapping = setup.get("outcome_mapping") or capture_spec.get("direction")
            outcome_mapping = outcome_mapping if isinstance(outcome_mapping, Mapping) else {}
            direction_value = (
                first_member.get("direction")
                or first_member.get("intended_outcome")
                or first_member.get("outcome")
                or setup.get("direction")
                or capture_spec.get("direction")
            )
            if isinstance(direction_value, Mapping):
                direction_value = (
                    direction_value.get("outcome")
                    or direction_value.get("direction")
                    or direction_value.get("side")
                )
            direction_text = str(direction_value or "").strip().casefold()
            for side_prefix in ("buy ", "sell "):
                if direction_text.startswith(side_prefix):
                    direction_text = direction_text[len(side_prefix) :].strip()
                    break
            direction_alias = direction_text.replace("-", "_").replace(" ", "_")
            if direction_alias in {"positive_delta", "positive", "up"}:
                direction_text = str(outcome_mapping.get("positive_delta") or "").strip().casefold()
            elif direction_alias in {"negative_delta", "negative", "down"}:
                direction_text = str(outcome_mapping.get("negative_delta") or "").strip().casefold()
            for side_prefix in ("buy ", "sell "):
                if direction_text.startswith(side_prefix):
                    direction_text = direction_text[len(side_prefix) :].strip()
                    break
            outcome = str(
                selected_materialized.get("outcome")
                or selected_materialized.get("outcome_name")
                or market_binding.get("outcome")
                or market_binding.get("outcome_name")
                or direction_text
                or ""
            ).strip().casefold()
            explicit_token = (
                market_binding.get("token_id")
                or selected_materialized.get("token_id")
            )
            if explicit_token not in (None, ""):
                requested_token = str(explicit_token).strip()
            else:
                token_ids = selected_materialized.get(
                    "token_ids",
                    selected_materialized.get("clob_token_ids", selected_materialized.get("tokens", {})),
                )
                token_ids = token_ids if isinstance(token_ids, Mapping) else {}
                token = token_ids.get(outcome) if outcome else None
                if token in (None, "") and outcome in {"yes", "yes_token", "affirmative"}:
                    token = selected_materialized.get("yes_token_id")
                if token in (None, "") and outcome in {"no", "no_token", "negative"}:
                    token = selected_materialized.get("no_token_id")
                if token not in (None, ""):
                    requested_token = str(token).strip()
                elif (
                    outcome in {"yes", "yes_token", "affirmative", "no", "no_token", "negative"}
                    and market_binding.get("token_id") not in (None, "")
                ):
                    requested_token = str(market_binding.get("token_id")).strip()
        market = diagnostics.get("market")
        market = dict(market) if isinstance(market, Mapping) else {}
        book = diagnostics.get("book", diagnostics.get("order_book"))
        book = dict(book) if isinstance(book, Mapping) else {}
        blockers: list[str] = []
        if not connectivity.get("ready") or str(connectivity.get("status") or "").upper() != "READY":
            blockers.append("CONNECTIVITY_NOT_READY")
        if not fresh:
            blockers.append("SELECTED_MARKET_READINESS_STALE")
        authentication = diagnostics.get("authentication")
        authentication = authentication if isinstance(authentication, Mapping) else {}
        if str(authentication.get("status") or "").upper() not in {"OK", "PASS"}:
            blockers.append("ACCOUNT_AUTHENTICATION_REQUIRED")
        account = diagnostics.get("account")
        account = account if isinstance(account, Mapping) else {}
        if account.get("authenticated") is not True:
            blockers.append("ACCOUNT_NOT_TRUSTED")
        geoblock = diagnostics.get("geoblock")
        geoblock = geoblock if isinstance(geoblock, Mapping) else {}
        if (
            not geoblock
            or geoblock.get("blocked") is not False
            or geoblock.get("close_only") is not False
        ):
            blockers.append("GEOGRAPHICALLY_BLOCKED")
        balance = diagnostics.get("balance")
        balance = balance if isinstance(balance, Mapping) else {}
        if str(balance.get("status") or "").upper() not in {"OK", "PASS"}:
            blockers.append("BALANCE_READINESS_REQUIRED")
        allowance = diagnostics.get("allowance")
        allowance = allowance if isinstance(allowance, Mapping) else {}
        if str(allowance.get("status") or "").upper() not in {"OK", "PASS", "SUFFICIENT"}:
            blockers.append("ALLOWANCE_READINESS_REQUIRED")
        observed_market = str(market.get("market_id") or market.get("id") or "").strip()
        observed_token = str(market.get("token_id") or "").strip()
        if not requested_market or not requested_token:
            blockers.append("SELECTED_MARKET_REQUIRED")
        if not observed_market or observed_market != requested_market:
            blockers.append("SELECTED_MARKET_CHANGED")
        if not observed_token or observed_token != requested_token:
            blockers.append("SELECTED_MARKET_TOKEN_CHANGED")
        if market.get("accepting_orders") is not True:
            blockers.append("SELECTED_MARKET_NOT_ACCEPTING_ORDERS")
        if not book.get("min_order_size") or not book.get("tick_size"):
            blockers.append("SELECTED_MARKET_MINIMUMS_REQUIRED")
        if not isinstance(book.get("bids"), (list, tuple)) or not book["bids"]:
            blockers.append("SELECTED_MARKET_BIDS_REQUIRED")
        if not isinstance(book.get("asks"), (list, tuple)) or not book["asks"]:
            blockers.append("SELECTED_MARKET_ASKS_REQUIRED")
        depth = book.get("depth_assessment")
        if (
            not isinstance(depth, Mapping)
            or str(depth.get("action") or "").strip().upper() != "SUITABLE"
        ):
            blockers.append("SELECTED_MARKET_DEPTH_REQUIRED")
        return {
            "status": "READY" if not blockers else "BLOCKED",
            "fresh": fresh,
            "checked_at": checked_at,
            "market_id": requested_market or None,
            "token_id": requested_token or None,
            "diagnostics": {
                "account": _safe_value(account),
                "geoblock": _safe_value(geoblock),
                "balance": _safe_value(balance),
                "allowance": _safe_value(allowance),
                "market": _safe_value(market),
                "book": _safe_value(book),
            },
            "blockers": list(dict.fromkeys(blockers)),
        }

    def _prepare_reviewed_proposed_selection(self) -> Mapping[str, Any] | None:
        """Materialize one immutable paper successor before authorization review."""
        loader = getattr(self.store, "load_current_portfolio_selection", None)
        committer = getattr(self.store, "commit_portfolio_selection", None)
        if not callable(loader) or not callable(committer):
            return None
        current = loader()
        if not isinstance(current, Mapping):
            return None
        activation = current.get("allocation_activation")
        if (
            isinstance(activation, Mapping)
            and str(activation.get("status") or "").upper() == "PREPARED"
        ):
            return current
        raw_members = current.get("members", current.get("selected_members", ()))
        if not isinstance(raw_members, (list, tuple)):
            return None
        proposal_members = [
            dict(member)
            for member in raw_members
            if isinstance(member, Mapping)
            and _canary_selection_member_is_proposed(member)
        ]
        if not proposal_members:
            return current
        if len(proposal_members) > 3:
            raise OperatorControlError("BOUNDED_ALLOCATION_REQUIRED")
        proposal_digest = self._rolling_canonical_hash(
            {
                "predecessor": current.get("selection_id")
                or current.get("portfolio_selection_id"),
                "members": [
                    {
                        "strategy_version_id": item.get("strategy_version_id"),
                        "candidate_id": item.get("candidate_id"),
                        "proposed_allocation": item.get("proposed_allocation"),
                    }
                    for item in proposal_members
                ],
                "risk_digest": current.get("proposed_allocation_risk_digest"),
            }
        )
        prepared_members: list[dict[str, Any]] = []
        for raw in raw_members:
            if not isinstance(raw, Mapping):
                continue
            item = dict(raw)
            if _canary_selection_member_is_proposed(item):
                item["allocation"] = str(item.get("proposed_allocation"))
                item["status"] = "PAPER"
                item["action"] = "OBSERVE"
                item["allocation_active"] = False
                item["paper_only"] = True
                item["canary_armed"] = False
            prepared_members.append(item)
        prepared = dict(current)
        predecessor_id = str(
            current.get("selection_id") or current.get("portfolio_selection_id") or ""
        ).strip()
        prepared_id = (
            "rolling-selection-prepared-"
            + self._rolling_canonical_hash(
                {"predecessor": predecessor_id, "proposal": proposal_digest}
            )[:40]
        )
        prepared.update(
            {
                "selection_id": prepared_id,
                "portfolio_selection_id": prepared_id,
                "members": prepared_members,
                "status": "PAPER",
                "paper_only": True,
                "allocation_active": False,
                "canary_armed": False,
                "k": 0,
                "supersedes_portfolio_selection_id": predecessor_id,
                "allocation_activation": {
                    "status": "PREPARED",
                    "proposal_digest": proposal_digest,
                    "predecessor_selection_id": predecessor_id,
                    "prepared_at": utc_now().isoformat(),
                },
            }
        )
        prepared["selection_hash"] = _canary_canonical_selection_hash(prepared)
        transaction_factory = getattr(self.store, "transaction", None)
        if callable(transaction_factory):
            try:
                transaction_context = transaction_factory(immediate=True)
            except TypeError:
                transaction_context = transaction_factory()
            with transaction_context:
                committed = committer(prepared, prepared_members)
        else:
            committed = committer(prepared, prepared_members)
        return committed if isinstance(committed, Mapping) else prepared
    def _fresh_proposed_risk_capacity(
        self,
        proposal_snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Recompute reviewed allocation capacity from current durable ledgers."""
        accounting_loader = getattr(self.store, "canary_risk_accounting", None)
        if not callable(accounting_loader):
            raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
        try:
            accounting = accounting_loader(now=utc_now())
        except TypeError:
            accounting = accounting_loader()
        except Exception as exc:
            raise OperatorControlError(
                "EXPLORATORY_LIVE_RISK_BINDING_STALE"
            ) from exc
        if not isinstance(accounting, Mapping):
            raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
        settings_snapshot = self.risk_settings_snapshot()
        if not isinstance(settings_snapshot, Mapping):
            raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
        expected_identity = {
            "risk_config_id": proposal_snapshot.get("risk_config_id"),
            "risk_config_generation": proposal_snapshot.get("risk_config_generation"),
            "risk_config_hash": proposal_snapshot.get("risk_config_hash"),
        }
        current_identity = {
            "risk_config_id": settings_snapshot.get(
                "risk_config_id", settings_snapshot.get("config_id")
            ),
            "risk_config_generation": settings_snapshot.get(
                "risk_config_generation", settings_snapshot.get("generation")
            ),
            "risk_config_hash": settings_snapshot.get(
                "risk_config_hash", settings_snapshot.get("config_hash")
            ),
        }
        for name, expected in expected_identity.items():
            if expected in (None, ""):
                continue
            if str(current_identity.get(name) or "") != str(expected):
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
        def decimal_value(value: Any, field: str) -> Decimal:
            if isinstance(value, bool) or value in (None, ""):
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            try:
                parsed = Decimal(str(value))
            except (InvalidOperation, TypeError, ValueError):
                raise OperatorControlError(
                    "EXPLORATORY_LIVE_RISK_BINDING_STALE"
                ) from None
            if not parsed.is_finite() or parsed < 0:
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            return parsed
        global_budget = decimal_value(
            proposal_snapshot.get("global_budget"), "global_budget"
        )
        raw_global_reserved = accounting.get("rolling_global_reserved_usd")
        global_reserved = decimal_value(raw_global_reserved, "rolling_global_reserved_usd")
        raw_reserved = accounting.get("rolling_strategy_reserved_usd")
        raw_allocations = accounting.get("rolling_strategy_allocations", {})
        if not isinstance(raw_reserved, Mapping) or not isinstance(raw_allocations, Mapping):
            raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
        reserved: dict[str, Decimal] = {}
        allocations: dict[str, Decimal] = {}
        for raw_id, raw_value in raw_reserved.items():
            identifier = str(raw_id).strip()
            if not identifier:
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            reserved[identifier] = decimal_value(raw_value, "rolling_strategy_reserved_usd")
        for raw_id, raw_value in raw_allocations.items():
            identifier = str(raw_id).strip()
            if not identifier:
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            allocations[identifier] = decimal_value(raw_value, "rolling_strategy_allocations")
        detail_total = sum(reserved.values(), Decimal("0"))
        if detail_total != global_reserved:
            raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
        uncovered = sum(
            (
                max(Decimal("0"), amount - allocations.get(identifier, Decimal("0")))
                for identifier, amount in reserved.items()
            ),
            Decimal("0"),
        )
        active_obligations = max(global_reserved, detail_total)
        available = max(Decimal("0"), global_budget - uncovered)
        raw_available = accounting.get("available_budget")
        if raw_available not in (None, ""):
            reported_available = decimal_value(raw_available, "available_budget")
            if reported_available != available:
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
        return {
            "global_budget": str(global_budget),
            "active_obligations": str(active_obligations),
            "uncovered_obligations": str(uncovered),
            "external_obligations": str(uncovered),
            "available_budget": str(available),
            "runtime_accounting_available": accounting.get(
                "runtime_accounting_available", True
            ),
        }

    def _capture_canary_selection_binding(self) -> dict[str, Any]:
        """Capture the singleton/config before a reviewed activation mutates it."""
        connection = getattr(self.store, "connection", None)
        if connection is None:
            raise OperatorControlError("EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED")
        table_present = False
        row_present = False
        candidate_id = ""
        try:
            table_present = (
                connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='canary_selection'"
                ).fetchone()
                is not None
            )
            if table_present:
                row = connection.execute(
                    "SELECT candidate_id FROM canary_selection WHERE singleton=1"
                ).fetchone()
                row_present = row is not None
                if row is not None:
                    try:
                        candidate_id = str(row["candidate_id"] or "").strip()
                    except (KeyError, TypeError):
                        candidate_id = str(row[0] or "").strip()
        except sqlite3.Error as exc:
            raise OperatorControlError(
                "EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED"
            ) from exc
        missing = object()
        getter = getattr(self.store, "get_operator_config", None)
        prior_binding = (
            getter("canary_selection_binding", missing)
            if callable(getter)
            else missing
        )
        return {
            "table_present": table_present,
            "row_present": row_present,
            "candidate_id": candidate_id,
            "binding_present": prior_binding is not missing,
            "binding": (
                dict(prior_binding)
                if isinstance(prior_binding, Mapping)
                else None
            ),
        }

    def _restore_canary_selection_binding(self, snapshot: Mapping[str, Any]) -> None:
        """Restore the exact pre-activation singleton and binding config."""
        connection = getattr(self.store, "connection", None)
        if connection is None:
            raise OperatorControlError("EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED")
        try:
            transaction_factory = getattr(self.store, "transaction", None)
            if callable(transaction_factory):
                try:
                    transaction_context = transaction_factory(immediate=True)
                except TypeError:
                    transaction_context = transaction_factory()
            else:
                transaction_context = nullcontext()
            with transaction_context:
                table_present = (
                    connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type='table' AND name='canary_selection'"
                    ).fetchone()
                    is not None
                )
                if table_present:
                    if bool(snapshot.get("row_present")):
                        candidate_id = str(snapshot.get("candidate_id") or "").strip() or None
                        current = connection.execute(
                            "SELECT singleton FROM canary_selection WHERE singleton=1"
                        ).fetchone()
                        if current is None:
                            now = utc_now().isoformat()
                            connection.execute(
                                "INSERT INTO canary_selection("
                                "singleton,ranking_run_id,candidate_id,rank,total_score,"
                                "component_scores_json,evidence_versions_json,reason,selected_at"
                                ") VALUES(1,?,?,?,?,?,?,?,?)",
                                (
                                    "restored",
                                    candidate_id,
                                    1,
                                    0.0,
                                    "{}",
                                    "{}",
                                    "restored pre-activation selection",
                                    now,
                                ),
                            )
                        else:
                            connection.execute(
                                "UPDATE canary_selection SET candidate_id=? WHERE singleton=1",
                                (candidate_id,),
                            )
                    else:
                        connection.execute(
                            "DELETE FROM canary_selection WHERE singleton=1"
                        )
                binding_present = bool(snapshot.get("binding_present"))
                if binding_present:
                    setter = getattr(self.store, "set_operator_config", None)
                    if not callable(setter):
                        raise OperatorControlError(
                            "EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED"
                        )
                    setter("canary_selection_binding", snapshot.get("binding"))
                else:
                    connection.execute(
                        "DELETE FROM operator_config WHERE config_key=?",
                        ("canary_selection_binding",),
                    )
        except OperatorControlError:
            raise
        except sqlite3.Error as exc:
            raise OperatorControlError(
                "EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED"
            ) from exc

    def _clear_canary_selection_binding_rollback(self) -> None:
        connection = getattr(self.store, "connection", None)
        if connection is None:
            return
        transaction_factory = getattr(self.store, "transaction", None)
        if callable(transaction_factory):
            try:
                transaction_context = transaction_factory(immediate=True)
            except TypeError:
                transaction_context = transaction_factory()
        else:
            transaction_context = nullcontext()
        with transaction_context:
            connection.execute(
                "DELETE FROM operator_config WHERE config_key=?",
                ("canary_selection_binding_rollback",),
            )

    def _synchronize_canary_selection_binding(
        self,
        *,
        candidate_id: str,
        selection_id: str,
        selection_hash: str,
    ) -> None:
        """Bind the shared canary singleton without replacing another winner."""
        connection = getattr(self.store, "connection", None)
        if connection is None:
            raise OperatorControlError("EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED")
        try:
            row = connection.execute(
                "SELECT candidate_id FROM canary_selection WHERE singleton=1"
            ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise OperatorControlError(
                    "EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED"
                ) from exc
            try:
                CanaryService(self.store, initialize=True, settings=self.settings)
                row = connection.execute(
                    "SELECT candidate_id FROM canary_selection WHERE singleton=1"
                ).fetchone()
            except Exception as init_exc:
                raise OperatorControlError(
                    "EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED"
                ) from init_exc
        except sqlite3.Error as exc:
            raise OperatorControlError(
                "EXPLORATORY_LIVE_CANARY_SELECTION_REQUIRED"
            ) from exc
        existing_candidate = ""
        if row is not None:
            try:
                existing_candidate = str(row["candidate_id"] or "").strip()
            except (KeyError, TypeError):
                existing_candidate = str(row[0] or "").strip()
            if existing_candidate and existing_candidate != candidate_id:
                raise OperatorControlError("EXPLORATORY_LIVE_CANARY_SELECTION_CONFLICT")
            if not existing_candidate:
                connection.execute(
                    "UPDATE canary_selection SET candidate_id=? WHERE singleton=1",
                    (candidate_id,),
                )
        else:
            now = utc_now().isoformat()
            connection.execute(
                "INSERT INTO canary_selection("
                "singleton,ranking_run_id,candidate_id,rank,total_score,"
                "component_scores_json,evidence_versions_json,reason,selected_at"
                ") VALUES(1,?,?,?,?,?,?,?,?)",
                (
                    "portfolio:" + selection_id,
                    candidate_id,
                    1,
                    0.0,
                    "{}",
                    "{}",
                    "portfolio exploratory review",
                    now,
                ),
            )
        setter = getattr(self.store, "set_operator_config", None)
        if callable(setter):
            setter(
                "canary_selection_binding",
                {
                    "candidate_id": candidate_id,
                    "selection_id": selection_id,
                    "selection_hash": selection_hash,
                    "bound_at": utc_now().isoformat(),
                },
            )


    def exploratory_live_review_snapshot(
        self, values: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """Build the final review disclosure without mutating state."""
        if values is not None and not isinstance(values, Mapping):
            raise OperatorControlError("EXPLORATORY_LIVE_VALUES_REQUIRED")
        raw = dict(values or {})
        self._prepare_reviewed_proposed_selection()
        context = self._authorization_context()
        selection = context.get("selection")
        selection = dict(selection) if isinstance(selection, Mapping) else {}
        policy = (
            selection.get("operating_policy")
            or selection.get("exploratory_policy")
            or selection.get("setup_policy")
        )
        if self._exploratory_live_policy_mode(policy) != "EXPLORATORY_LIVE":
            raise OperatorControlError("EXPLORATORY_LIVE_POLICY_REQUIRED")
        members_raw = selection.get("members", selection.get("selected_members", ()))
        members = [item for item in members_raw if isinstance(item, Mapping)] if isinstance(members_raw, (list, tuple)) else []
        funded: list[Mapping[str, Any]] = []
        rejected: list[Mapping[str, Any]] = []
        for member in members[:10]:
            status = str(member.get("status") or member.get("stage") or "").upper()
            if bool(member.get("rejected")) or status == "REJECTED":
                rejected.append(member)
            elif (
                _canary_selection_member_is_funded(member)
                or _canary_selection_member_is_proposed(member)
            ):
                funded.append(member)
        reviewed_candidate = str(raw.get("candidate_id") or "").strip()
        server_candidate = (
            str(funded[0].get("candidate_id") or "").strip() if funded else ""
        )
        if reviewed_candidate and reviewed_candidate != server_candidate:
            raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
        limits = self._rolling_effective_limits()
        expected_limits = {
            "max_all_in_buy_usd": "1.00",
            "max_fee_reserve_usd": "0.01",
            "max_gross_daily_buy_usd": "5.00",
            "max_aggregate_open_cost_usd": "5.00",
            "max_aggregate_exposure_usd": "5.00",
            "max_positions": 3,
            "max_submitted_orders_per_day": 5,
            "realized_loss_entry_stop_usd": "2.00",
            "equity_loss_entry_stop_usd": "2.00",
            "max_slippage_bps": 100,
        }
        limit_blockers = [
            name for name, expected in expected_limits.items()
            if str(limits.get(name)) != str(expected)
        ]
        binding_blockers: list[str] = []
        if any(
            context.get(field) in (None, "")
            for field in ("policy_id", "policy_version", "policy_hash")
        ):
            binding_blockers.append("EXPLORATORY_SETUP_BINDING_REQUIRED")
        for member in funded:
            setup = member.get("operational_setup")
            setup = setup if isinstance(setup, Mapping) else {}
            setup_id = member.get("setup_id") or setup.get("setup_id")
            setup_version = (
                member.get("setup_version")
                or setup.get("setup_version")
                or setup.get("contract_version")
            )
            setup_hash = (
                member.get("operational_setup_hash")
                or member.get("setup_hash")
                or setup.get("operational_setup_hash")
                or setup.get("setup_hash")
            )
            if any(value in (None, "") for value in (setup_id, setup_version, setup_hash)):
                binding_blockers.append("EXPLORATORY_SETUP_BINDING_REQUIRED")
                break
        setups = []
        for member in funded[:3]:
            setup = member.get("operational_setup")
            setup = setup if isinstance(setup, Mapping) else {}
            outcome_mapping = setup.get("outcome_mapping")
            holding_semantics = setup.get("holding_semantics")
            setups.append(
                {
                    "strategy_version_id": str(member.get("strategy_version_id") or "").strip() or None,
                    "candidate_id": str(member.get("candidate_id") or "").strip() or None,
                    "setup_id": setup.get("setup_id"),
                    "setup_version": setup.get("setup_version") or setup.get("contract_version"),
                    "setup_hash": member.get(
                        "operational_setup_hash",
                        setup.get("operational_setup_hash", setup.get("setup_hash")),
                    ),
                    "entry_predicate": _safe_value(setup.get("entry_predicate")),
                    "outcome_mapping": _safe_value(outcome_mapping),
                    "direction": _safe_value(outcome_mapping),
                    "sizing": _safe_value(setup.get("sizing")),
                    "holding_semantics": _safe_value(holding_semantics),
                    "exit_semantics": _safe_value(
                        setup.get("exit_semantics", setup.get("exit_policy", holding_semantics))
                    ),
                    "lookback": setup.get("lookback"),
                }
            )
        adverse = [
            {
                "strategy_version_id": str(member.get("strategy_version_id") or "").strip() or None,
                "verdict": "REJECTED",
                "evidence": _safe_value(member.get("adverse_evidence", member.get("evidence", {}))),
            }
            for member in rejected[:16]
        ]
        scope = context.get("scope")
        scope = scope if isinstance(scope, Mapping) else {}
        readiness = self._selected_market_readiness(
            selection,
            scope=scope,
            target_candidate_id=server_candidate or None,
        )
        authorization_snapshot = self.execution_authorization_snapshot()
        authorization = authorization_snapshot.get("authorization")
        authorization = authorization if isinstance(authorization, Mapping) else {}
        persisted_lifetime = authorization.get(
            "lifetime_budget", authorization.get("lifetime_budget_json")
        )
        persisted_expiry = authorization.get("expires_at")
        persisted_stops = authorization.get(
            "stop_rules", authorization.get("stop_rules_json")
        )
        settings_snapshot = self.risk_settings_snapshot()
        authorization_blockers: list[str] = []
        if persisted_lifetime in (None, "", {}):
            authorization_blockers.append("EXPLORATORY_LIVE_LIFETIME_BUDGET_REQUIRED")
        if persisted_expiry in (None, ""):
            authorization_blockers.append("EXPLORATORY_LIVE_EXPIRES_AT_REQUIRED")
        if not isinstance(persisted_stops, Mapping) or not persisted_stops:
            authorization_blockers.append("EXPLORATORY_LIVE_STOP_RULES_REQUIRED")
        authoritative_usage = settings_snapshot.get("usage")
        authoritative_usage = (
            dict(authoritative_usage)
            if isinstance(authoritative_usage, Mapping)
            else {}
        )
        reserved_exit_capacity = authoritative_usage.get(
            "pending_sell_quantity_by_market",
            authoritative_usage.get("reserved_exit_capacity", {}),
        )
        return {
            "profitability": "UNPROVEN",
            "paper_only": True,
            "live_execution": False,
            "policy": _safe_value(policy),
            "selected_setups": setups,
            "entry_predicate": [item.get("entry_predicate") for item in setups],
            "direction": [item.get("direction") for item in setups],
            "sizing": [item.get("sizing") for item in setups],
            "exit": [item.get("exit_semantics") for item in setups],
            "lookback": [item.get("lookback") for item in setups],
            "adverse_evidence": adverse,
            "scope": {
                "scope_hash": context.get("scope_hash"),
                "scope_version": context.get("scope_version"),
                "market_ids": list(scope.get("market_ids", scope.get("exact_market_ids", ())))[:16],
            },
            "members": [
                {
                    "strategy_version_id": item.get("strategy_version_id"),
                    "candidate_id": item.get("candidate_id"),
                    "allocation": item.get("sizing"),
                }
                for item in setups
            ],
            "limits": dict(limits),
            "limit_blockers": limit_blockers,
            "allocation": {
                "selected_members": len(funded),
                "maximum_members": 3,
                "bounded": 1 <= len(funded) <= 3,
            },
            "readiness": readiness,
            "authorization": authorization_snapshot,
            "authorization_bindings": {
                "selection_id": context.get("selection_id"),
                "selection_hash": context.get("selection_hash"),
                "policy_id": context.get("policy_id"),
                "policy_version": context.get("policy_version"),
                "policy_hash": context.get("policy_hash"),
                "setup_bindings": _safe_value(context.get("setup_bindings", [])),
                "proposed_allocation_total": selection.get(
                    "proposed_allocation_total"
                ),
                "proposed_allocation_risk_digest": selection.get(
                    "proposed_allocation_risk_digest"
                ),
            },
            "lifetime_budget": _safe_value(persisted_lifetime),
            "expires_at": persisted_expiry,
            "stop_rules": _safe_value(persisted_stops),
            "accounting": {
                "buy_pending_usd": authoritative_usage.get("buy_pending_usd"),
                "buy_unknown_usd": authoritative_usage.get("buy_unknown_usd"),
                "all_in_buy_reserved_usd": authoritative_usage.get("all_in_buy_reserved_usd"),
                "reserved_exit_capacity": _safe_value(reserved_exit_capacity),
                "proposed_allocation_total": selection.get(
                    "proposed_allocation_total"
                ),
                "proposed_allocation_risk_digest": selection.get(
                    "proposed_allocation_risk_digest"
                ),
                "equity_status": authoritative_usage.get("equity_status"),
            },
            "blockers": list(
                dict.fromkeys(
                    [
                        *authorization_blockers,
                        *limit_blockers,
                        *binding_blockers,
                        *readiness.get("blockers", []),
                        *([] if 1 <= len(funded) <= 3 else ["BOUNDED_ALLOCATION_REQUIRED"]),
                    ]
                )
            ),
        }

    def _activate_reviewed_proposed_selection(
        self,
        *,
        context: Mapping[str, Any],
        authorization: Mapping[str, Any],
        actor: str,
    ) -> Mapping[str, Any] | None:
        """Append the exact reviewed proposal as the active selection.

        Selection rows are immutable.  Activation therefore commits one
        successor pointer in the same store transaction after the canary has
        passed all existing fences; failures leave the paper proposal intact.
        """
        loader = getattr(self.store, "load_current_portfolio_selection", None)
        committer = getattr(self.store, "commit_portfolio_selection", None)
        if not callable(loader) or not callable(committer):
            raise OperatorControlError("EXPLORATORY_LIVE_SELECTION_STORAGE_UNAVAILABLE")
        current = loader()
        if current is None:
            return None
        if not isinstance(current, Mapping):
            raise OperatorControlError("EXPLORATORY_LIVE_SELECTION_REQUIRED")
        expected_selection = str(context.get("selection_id") or "").strip()
        current_selection = str(
            current.get("selection_id") or current.get("portfolio_selection_id") or ""
        ).strip()
        if expected_selection and current_selection != expected_selection:
            raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
        raw_members = current.get("members", current.get("selected_members", ()))
        if not isinstance(raw_members, (list, tuple)):
            raise OperatorControlError("EXPLORATORY_LIVE_SELECTION_REQUIRED")
        proposal_risk_snapshot = current.get("proposed_allocation_risk_snapshot")
        proposal_risk_digest = str(
            current.get("proposed_allocation_risk_digest") or ""
        ).strip()
        if proposal_risk_digest:
            if not isinstance(proposal_risk_snapshot, Mapping):
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            if self._rolling_canonical_hash(proposal_risk_snapshot) != proposal_risk_digest:
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            expected_risk_digest = str(
                context.get("proposed_allocation_risk_digest") or ""
            ).strip()
            if expected_risk_digest and expected_risk_digest != proposal_risk_digest:
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            try:
                snapshot_capacity = Decimal(
                    str(proposal_risk_snapshot.get("available_budget", "0"))
                )
            except (InvalidOperation, TypeError, ValueError):
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE") from None
            if not snapshot_capacity.is_finite() or snapshot_capacity < 0:
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            fresh_capacity = self._fresh_proposed_risk_capacity(proposal_risk_snapshot)
            for field in (
                "global_budget",
                "active_obligations",
                "uncovered_obligations",
                "external_obligations",
                "available_budget",
            ):
                try:
                    reviewed_value = Decimal(str(proposal_risk_snapshot.get(field)))
                    current_value = Decimal(str(fresh_capacity.get(field)))
                except (InvalidOperation, TypeError, ValueError):
                    raise OperatorControlError(
                        "EXPLORATORY_LIVE_RISK_BINDING_STALE"
                    ) from None
                if reviewed_value != current_value:
                    raise OperatorControlError(
                        "EXPLORATORY_LIVE_RISK_CAPACITY_CHANGED"
                    )
            if bool(
                proposal_risk_snapshot.get("runtime_accounting_available", True)
            ) != bool(fresh_capacity.get("runtime_accounting_available", True)):
                raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
        else:
            snapshot_capacity = None
        proposal_members = [
            dict(member)
            for member in raw_members
            if isinstance(member, Mapping)
            and _canary_selection_member_is_proposed(member)
        ]
        if not proposal_members:
            return current
        if len(proposal_members) > 3:
            raise OperatorControlError("BOUNDED_ALLOCATION_REQUIRED")
        proposal_total = sum(
            (
                Decimal(
                    self._authorization_decimal(
                        member.get("proposed_allocation"),
                        "proposed_allocation",
                    )
                )
                for member in proposal_members
            ),
            Decimal("0"),
        )
        if proposal_total <= 0 or (
            snapshot_capacity is not None and proposal_total > snapshot_capacity
        ):
            raise OperatorControlError("EXPLORATORY_LIVE_RISK_CAPACITY_CHANGED")
        declared_total = current.get("proposed_allocation_total")
        if declared_total not in (None, ""):
            try:
                if self._authorization_decimal(declared_total, "proposed_allocation_total") != proposal_total:
                    raise OperatorControlError("EXPLORATORY_LIVE_RISK_BINDING_STALE")
            except OperatorControlError:
                raise
        current_activation = current.get("allocation_activation")
        if (
            isinstance(current_activation, Mapping)
            and str(current_activation.get("status") or "").upper() == "PREPARED"
        ):
            active_members: list[dict[str, Any]] = []
            for member in raw_members:
                if not isinstance(member, Mapping):
                    continue
                item = dict(member)
                if _canary_selection_member_is_proposed(item):
                    item.pop("proposed_allocation", None)
                    item["allocation_active"] = True
                    item["paper_only"] = False
                    item["canary_armed"] = True
                    item["status"] = "ACTIVE"
                    item["action"] = "HOLD"
                active_members.append(item)
            activated_at = utc_now().isoformat()
            activation = {
                "status": "ACTIVE",
                "actor": str(actor).strip() or "operator",
                "authorization_id": authorization.get("authorization_id")
                or authorization.get("id"),
                "proposal_digest": current_activation.get("proposal_digest"),
                "prepared_selection_id": current_selection,
                "activated_at": activated_at,
                "risk_digest": proposal_risk_digest,
            }
            setter = getattr(self.store, "set_operator_config", None)
            if not callable(setter):
                raise OperatorControlError("EXPLORATORY_LIVE_SELECTION_STORAGE_UNAVAILABLE")
            activation_payload = {
                "selection_id": current_selection,
                "status": "ACTIVE",
                "paper_only": False,
                "allocation_active": True,
                "canary_armed": True,
                "k": sum(
                    1
                    for item in active_members
                    if str(item.get("status") or "").upper() in {"ACTIVE", "REDUCE"}
                    and Decimal(str(item.get("allocation") or "0")) > 0
                ),
                "members": active_members,
                "allocation_activation": activation,
            }
            review_loader = getattr(self.store, "load_portfolio_review_state", None)
            review_saver = getattr(self.store, "save_portfolio_review_state", None)
            review = review_loader() if callable(review_loader) else None
            review_payload = dict(review) if isinstance(review, Mapping) else {}
            review_payload.update(
                {
                    "portfolio_selection_id": current_selection,
                    "status": "ACTIVE",
                    "allocation_activation": activation,
                    "updated_at": activated_at,
                }
            )
            binding_before = self._capture_canary_selection_binding()
            rollback_marker = {
                "status": "PENDING",
                "selection_id": current_selection,
                "binding": binding_before,
                "created_at": activated_at,
            }
            active_candidate = next(
                (
                    str(item.get("candidate_id") or "").strip()
                    for item in active_members
                    if isinstance(item, Mapping)
                    and str(item.get("candidate_id") or "").strip()
                    and item.get("allocation_active") is True
                ),
                "",
            )
            active_hash = _canary_authorization_current_selection_hash(current)
            if not active_candidate:
                raise OperatorControlError("EXPLORATORY_LIVE_CANDIDATE_REQUIRED")
            transaction_factory = getattr(self.store, "transaction", None)
            if callable(transaction_factory):
                try:
                    transaction_context = transaction_factory(immediate=True)
                except TypeError:
                    transaction_context = transaction_factory()
            else:
                transaction_context = nullcontext()
            with transaction_context:
                setter("rolling_selection_activation", activation_payload)
                if callable(review_saver):
                    review_saver(review_payload)
                setter("canary_selection_binding_rollback", rollback_marker)
                self._synchronize_canary_selection_binding(
                    candidate_id=active_candidate,
                    selection_id=current_selection,
                    selection_hash=active_hash,
                )
            active_view = dict(current)
            active_view.update(activation_payload)
            active_view["_canary_selection_binding_before"] = binding_before
            return active_view
        proposal_digest = self._rolling_canonical_hash(
            {
                "selection_id": current_selection,
                "policy_id": context.get("policy_id"),
                "policy_version": context.get("policy_version"),
                "policy_hash": context.get("policy_hash"),
                "active_settings_hash": context.get("active_settings_hash"),
                "active_settings_generation": context.get("active_settings_generation"),
                "members": [
                    {
                        "strategy_version_id": member.get("strategy_version_id"),
                        "candidate_id": member.get("candidate_id"),
                        "research_trial_id": member.get("research_trial_id"),
                        "proposed_allocation": member.get("proposed_allocation"),
                    }
                    for member in proposal_members
                ],
            }
        )
        active_members: list[dict[str, Any]] = []
        for member in raw_members:
            if not isinstance(member, Mapping):
                continue
            item = dict(member)
            if _canary_selection_member_is_proposed(item):
                proposed = self._authorization_decimal(
                    item.get("proposed_allocation"),
                    "proposed_allocation",
                )
                item.pop("proposed_allocation", None)
                item["allocation"] = proposed
                item["allocation_active"] = True
                item["paper_only"] = False
                item["canary_armed"] = True
                item["status"] = "ACTIVE"
                item["action"] = "HOLD"
            active_members.append(item)
        activated_at = utc_now().isoformat()
        successor = dict(current)
        successor_id = (
            "rolling-selection-active-"
            + self._rolling_canonical_hash(
                {"predecessor": current_selection, "proposal": proposal_digest}
            )[:40]
        )
        successor.update(
            {
                "portfolio_selection_id": successor_id,
                "selection_id": successor_id,
                "members": active_members,
                "status": "ACTIVE",
                "paper_only": False,
                "canary_armed": True,
                "k": sum(
                    1
                    for member in active_members
                    if str(member.get("status", "")).upper() in {"ACTIVE", "REDUCE"}
                    and Decimal(str(member.get("allocation", "0"))) > 0
                ),
                "supersedes_portfolio_selection_id": current_selection,
                "allocation_activation": {
                    "status": "ACTIVE",
                    "actor": str(actor).strip() or "operator",
                    "authorization_id": authorization.get("authorization_id")
                    or authorization.get("id"),
                    "proposal_digest": proposal_digest,
                    "activated_at": activated_at,
                    "policy_id": context.get("policy_id"),
                    "policy_version": context.get("policy_version"),
                    "policy_hash": context.get("policy_hash"),
                    "active_settings_hash": context.get("active_settings_hash"),
                    "active_settings_generation": context.get(
                        "active_settings_generation"
                    ),
                },
                "selected_at": activated_at,
                "review_due_at": activated_at,
            }
        )
        review_loader = getattr(self.store, "load_portfolio_review_state", None)
        review_saver = getattr(self.store, "save_portfolio_review_state", None)
        review = review_loader() if callable(review_loader) else None
        review_payload = dict(review) if isinstance(review, Mapping) else {}
        review_payload.update(
            {
                "portfolio_selection_id": successor_id,
                "status": "ACTIVE",
                "updated_at": activated_at,
                "allocation_activation": successor["allocation_activation"],
            }
        )
        transaction_factory = getattr(self.store, "transaction", None)
        if callable(transaction_factory):
            try:
                transaction_context = transaction_factory(immediate=True)
            except TypeError:
                transaction_context = transaction_factory()
            with transaction_context:
                committed = committer(successor, active_members)
                if callable(review_saver):
                    review_saver(review_payload)
        else:
            committed = committer(successor, active_members)
            if callable(review_saver):
                review_saver(review_payload)
        return committed if isinstance(committed, Mapping) else successor

    def confirm_exploratory_live(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        actor: str = "operator",
    ) -> dict[str, Any]:
        """Coordinate one reviewed authorization through arm and enable fences."""
        if values is not None and not isinstance(values, Mapping):
            raise OperatorControlError("EXPLORATORY_LIVE_VALUES_REQUIRED")
        raw = dict(values or {})
        snapshot = self.execution_authorization_snapshot()
        active = snapshot.get("active")
        draft = snapshot.get("draft")
        active = dict(active) if isinstance(active, Mapping) else None
        draft = dict(draft) if isinstance(draft, Mapping) else None
        auth = active or draft
        if not isinstance(auth, Mapping):
            raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_REQUIRED")
        auth = dict(auth)
        if auth.get("lifetime_budget") in (None, "", {}):
            auth["lifetime_budget"] = auth.get("lifetime_budget_json")
        if auth.get("stop_rules") in (None, "", {}):
            auth["stop_rules"] = auth.get("stop_rules_json")
        status = str(auth.get("status") or "").upper()
        if status not in {"DRAFT", "ACTIVE"}:
            raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_REQUIRED")
        for field in ("lifetime_budget", "expires_at", "stop_rules"):
            if auth.get(field) in (None, "", {}):
                raise OperatorControlError(f"EXPLORATORY_LIVE_{field.upper()}_REQUIRED")
        lifetime = self._authorization_decimal(auth["lifetime_budget"], "lifetime_budget")
        expiry_text = self._authorization_timestamp(auth["expires_at"], "expires_at")
        expiry = datetime.fromisoformat(expiry_text)
        if expiry <= utc_now():
            raise OperatorControlError("EXPLORATORY_LIVE_EXPIRED")
        if not isinstance(auth.get("stop_rules"), Mapping) or not auth["stop_rules"]:
            raise OperatorControlError("EXPLORATORY_LIVE_STOP_RULES_REQUIRED")
        if not (
            auth["stop_rules"].get("halt_on_unknown_execution") is True
            or auth["stop_rules"].get("on_unknown_execution") is True
            or str(auth["stop_rules"].get("on_any_blocker") or "").strip().upper() == "STOP"
        ):
            raise OperatorControlError("EXPLORATORY_LIVE_UNKNOWN_STOP_REQUIRED")
        review = self.exploratory_live_review_snapshot(raw)
        if review["blockers"]:
            raise OperatorControlError(str(review["blockers"][0]))
        context = self._authorization_context()
        for field in ("selection_id", "selection_hash", "policy_id", "policy_version", "policy_hash"):
            expected = context.get(field)
            observed = auth.get(field)
            if expected not in (None, "") and str(observed or "") != str(expected):
                raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
        expected_setups = context.get("setup_bindings", [])
        observed_setups = auth.get("setup_bindings", [])
        if expected_setups:
            if not isinstance(observed_setups, (list, tuple)):
                raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
            try:
                expected_encoded = json.dumps(
                    expected_setups, sort_keys=True, separators=(",", ":"), default=str
                )
                observed_encoded = json.dumps(
                    observed_setups, sort_keys=True, separators=(",", ":"), default=str
                )
            except (TypeError, ValueError):
                raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE") from None
            if observed_encoded != expected_encoded:
                raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
        expected_versions = {
            str(item).strip()
            for item in context.get("strategy_versions", ())
            if str(item).strip()
        }
        observed_versions = auth.get(
            "exact_strategy_versions", auth.get("strategy_version_ids", ())
        )
        if isinstance(observed_versions, str):
            observed_versions = (observed_versions,)
        if (
            {
                str(item).strip()
                for item in (observed_versions or ())
                if str(item).strip()
            }
            != expected_versions
        ):
            raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
        for field in (
            "scope_hash",
            "scope_version",
            "active_settings_hash",
            "proposed_allocation_total",
            "proposed_allocation_risk_digest",
        ):
            if str(auth.get(field) or "") != str(context.get(field) or ""):
                raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
        try:
            if int(auth.get("active_settings_generation") or 0) != int(context.get("active_settings_generation") or 0):
                raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
        except (TypeError, ValueError, OverflowError):
            raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE") from None
        activated_new = False
        armed_new = False
        service: CanaryService | None = None
        def _encoded(value: Any) -> str:
            return json.dumps(
                _safe_value(value),
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )

        def _candidate_ids(binding_context: Mapping[str, Any]) -> set[str]:
            identifiers: set[str] = set()
            selection_value = binding_context.get("selection")
            selection_value = (
                selection_value if isinstance(selection_value, Mapping) else {}
            )
            members_value = selection_value.get(
                "members", selection_value.get("selected_members", ())
            )
            if isinstance(members_value, (list, tuple)):
                for member in members_value:
                    if isinstance(member, Mapping):
                        identifier = str(member.get("candidate_id") or "").strip()
                        if identifier:
                            identifiers.add(identifier)
            setup_values = binding_context.get("setup_bindings", ())
            if isinstance(setup_values, (list, tuple)):
                for binding in setup_values:
                    if isinstance(binding, Mapping):
                        identifier = str(binding.get("candidate_id") or "").strip()
                        if identifier:
                            identifiers.add(identifier)
            return identifiers

        def _same_reviewed_context(
            expected: Mapping[str, Any], observed: Mapping[str, Any]
        ) -> bool:
            for field in (
                "selection_id",
                "selection_hash",
                "policy_id",
                "policy_version",
                "policy_hash",
                "setup_bindings",
                "strategy_versions",
                "scope_hash",
                "scope_version",
                "active_settings_hash",
                "active_settings_generation",
                "proposed_allocation_total",
                "proposed_allocation_risk_digest",
            ):
                if _encoded(expected.get(field)) != _encoded(observed.get(field)):
                    return False
            return True

        candidate_id = str(
            raw.get("candidate_id")
            or (review.get("members") or [{}])[0].get("candidate_id")
            or ""
        ).strip()
        if not candidate_id:
            raise OperatorControlError("EXPLORATORY_LIVE_CANDIDATE_REQUIRED")
        selection_value = context.get("selection")
        selection_value = (
            selection_value if isinstance(selection_value, Mapping) else {}
        )
        selected_members = selection_value.get(
            "members", selection_value.get("selected_members", ())
        )
        server_candidate_id = ""
        if isinstance(selected_members, (list, tuple)):
            for member in selected_members[:10]:
                if not isinstance(member, Mapping):
                    continue
                status_value = str(
                    member.get("status") or member.get("stage") or ""
                ).strip().upper()
                if bool(member.get("rejected")) or status_value == "REJECTED":
                    continue
                if (
                    _canary_selection_member_is_funded(member)
                    or _canary_selection_member_is_proposed(member)
                ):
                    server_candidate_id = str(
                        member.get("candidate_id") or ""
                    ).strip()
                    break
        if server_candidate_id and candidate_id != server_candidate_id:
            raise OperatorControlError(
                "EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE"
            )
        reviewed_candidate_ids = {
            str(member.get("candidate_id") or "").strip()
            for member in (review.get("members") or ())
            if isinstance(member, Mapping)
            and str(member.get("candidate_id") or "").strip()
        }
        if reviewed_candidate_ids and candidate_id not in reviewed_candidate_ids:
            raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")
        current_candidate_ids = _candidate_ids(context)
        if current_candidate_ids and candidate_id not in current_candidate_ids:
            raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE")

        def _verify_control_identity(
            status_document: Any, *, require_control: bool
        ) -> None:
            if not isinstance(status_document, Mapping):
                if require_control:
                    raise OperatorControlError(
                        "EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE"
                    )
                return
            control_candidate = str(
                status_document.get("control_candidate")
                or status_document.get("candidate")
                or ""
            ).strip()
            selected_candidate = str(
                status_document.get("selected_candidate")
                or status_document.get("winner_id")
                or ""
            ).strip()
            if require_control and control_candidate != candidate_id:
                raise OperatorControlError(
                    "EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE"
                )
            if selected_candidate and selected_candidate != candidate_id:
                raise OperatorControlError(
                    "EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE"
                )

        auth_id = str(auth.get("authorization_id") or auth.get("id") or "").strip()
        activated_generation = auth.get("generation")
        active_selection: Mapping[str, Any] | None = None
        try:
            if status == "DRAFT":
                activated = self.activate_execution_authorization(
                    auth_id, actor=actor, expected_generation=auth.get("generation")
                )
                activated_new = True
                activated_auth = dict(activated.get("authorization") or {})
                activated_auth_id = str(
                    activated_auth.get("authorization_id") or activated_auth.get("id") or auth_id
                ).strip()
                activated_generation = activated_auth.get(
                    "generation", auth.get("generation")
                )
                auth_id = activated_auth_id
                reviewed_bindings = {
                    field: auth.get(field)
                    for field in (
                        "selection_id",
                        "selection_hash",
                        "policy_id",
                        "policy_version",
                        "policy_hash",
                        "setup_bindings",
                        "strategy_versions",
                        "exact_strategy_versions",
                        "strategy_version_ids",
                        "scope_hash",
                        "scope_version",
                        "active_settings_hash",
                        "active_settings_generation",
                        "proposed_allocation_total",
                        "proposed_allocation_risk_digest",
                    )
                }
                for field, expected in reviewed_bindings.items():
                    if expected in (None, "", [], {}):
                        continue
                    observed = activated_auth.get(field)
                    if observed not in (None, "", [], {}) and field in {
                        "selection_id",
                        "selection_hash",
                        "policy_id",
                        "policy_version",
                        "policy_hash",
                        "setup_bindings",
                    }:
                        try:
                            if json.dumps(observed, sort_keys=True, separators=(",", ":"), default=str) != json.dumps(
                                expected, sort_keys=True, separators=(",", ":"), default=str
                            ):
                                raise OperatorControlError(
                                    "EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE"
                                )
                        except (TypeError, ValueError):
                            raise OperatorControlError(
                                "EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE"
                            ) from None
                    activated_auth.setdefault(field, expected)
                auth = activated_auth
            if str(auth.get("status") or "").upper() != "ACTIVE":
                raise OperatorControlError("EXPLORATORY_LIVE_AUTHORIZATION_REQUIRED")
            settings_snapshot = self.settings.snapshot()
            config_id = settings_snapshot.get("config_id")
            generation = settings_snapshot.get("generation")
            if not isinstance(config_id, str) or not config_id.strip():
                raise OperatorControlError("CANARY_SETTINGS_CONFIG_REQUIRED")
            if isinstance(generation, bool):
                raise OperatorControlError("CANARY_SETTINGS_GENERATION_REQUIRED")
            try:
                generation = int(generation)
            except (TypeError, ValueError, OverflowError):
                raise OperatorControlError("CANARY_SETTINGS_GENERATION_REQUIRED") from None
            if generation < 1:
                raise OperatorControlError("CANARY_SETTINGS_GENERATION_REQUIRED")
            credentials = CredentialStore()
            venue = PolymarketClobV2Venue(allow_environment=False)
            service = CanaryService(
                self.store, credentials=credentials, initialize=True, settings=self.settings
            )
            status_document = service.authoritative_status()
            state = str(
                status_document.get("micro_live_canary")
                if isinstance(status_document, Mapping)
                else ""
            ).upper()
            current_context = self._authorization_context()
            current_ids = _candidate_ids(current_context)
            context_changed = not _same_reviewed_context(context, current_context)
            candidate_changed = bool(current_ids) and candidate_id not in current_ids
            if context_changed or candidate_changed:
                if state in {"ARMED", "AUTONOMOUS_MICRO_LIVE"}:
                    service.disarm()
                raise OperatorControlError(
                    "EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE"
                )
            identity_keys = {
                "control_candidate",
                "candidate",
                "selected_candidate",
                "winner_id",
            }
            identity_present = isinstance(status_document, Mapping) and bool(
                identity_keys.intersection(status_document)
            )
            if state in {"ARMED", "AUTONOMOUS_MICRO_LIVE"}:
                if not identity_present:
                    service.disarm()
                    state = "DISARMED"
                else:
                    try:
                        _verify_control_identity(status_document, require_control=True)
                    except OperatorControlError:
                        service.disarm()
                        state = "DISARMED"
            if state not in {"ARMED", "AUTONOMOUS_MICRO_LIVE"}:
                armed = service.arm(
                    candidate_id,
                    venue=venue,
                    config_id=str(config_id),
                    expected_generation=generation,
                    credentials_configured=True,
                )
                armed_new = True
                if isinstance(armed, Mapping):
                    _verify_control_identity(armed, require_control=True)
            final_context = self._authorization_context()
            final_ids = _candidate_ids(final_context)
            if (
                not _same_reviewed_context(context, final_context)
                or (final_ids and candidate_id not in final_ids)
            ):
                service.disarm()
                armed_new = False
                raise OperatorControlError(
                    "EXPLORATORY_LIVE_AUTHORIZATION_BINDING_STALE"
                )
            final_status = service.authoritative_status()
            final_state = str(
                final_status.get("micro_live_canary")
                if isinstance(final_status, Mapping)
                else ""
            ).upper()
            if final_state in {"ARMED", "AUTONOMOUS_MICRO_LIVE"}:
                try:
                    _verify_control_identity(final_status, require_control=True)
                except OperatorControlError:
                    service.disarm()
                    armed_new = False
                    raise
            active_selection = self._activate_reviewed_proposed_selection(
                context=context,
                authorization=auth,
                actor=actor,
            )
            if active_selection is not None and not isinstance(active_selection, Mapping):
                raise OperatorControlError("EXPLORATORY_LIVE_SELECTION_REQUIRED")
            enabled = service.enable_autonomous_micro_live(
                venue="polymarket",
                config_id=str(config_id),
                expected_generation=generation,
                expected_credential_fingerprint=credential_fingerprint(
                    credentials.load(allow_environment=False)
                ),
                expected_candidate_id=candidate_id,
            )
            try:
                _verify_control_identity(enabled, require_control=True)
                post_enable_status = service.authoritative_status()
                _verify_control_identity(post_enable_status, require_control=True)
            except OperatorControlError:
                service.disarm()
                armed_new = False
                raise
            self._clear_canary_selection_binding_rollback()
            return {
                "status": "AUTONOMOUS_MICRO_LIVE",
                "authorization": _safe_value(auth),
                "review": review,
                "lifetime_budget": lifetime,
                "expires_at": expiry_text,
                "stop_rules": _safe_value(auth["stop_rules"]),
                "canary": _safe_value(enabled),
                "paper_only": False,
                "live_execution": True,
            }
        except Exception as original_error:
            rollback_failures: list[str] = []
            if isinstance(active_selection, Mapping):
                binding_before = active_selection.get("_canary_selection_binding_before")
                if isinstance(binding_before, Mapping):
                    try:
                        self._restore_canary_selection_binding(binding_before)
                    except Exception as exc:
                        rollback_failures.append(
                            f"CANARY_SELECTION_ROLLBACK_FAILED:{type(exc).__name__}"
                        )
                setter = getattr(self.store, "set_operator_config", None)
                if callable(setter):
                    prepared_members = []
                    for raw_member in active_selection.get("members", ()):
                        if not isinstance(raw_member, Mapping):
                            continue
                        item = dict(raw_member)
                        allocation = item.get("allocation")
                        if (
                            item.get("allocation_active") is True
                            and allocation not in (None, "", "0", 0)
                        ):
                            item["proposed_allocation"] = str(allocation)
                        item["status"] = "PAPER"
                        item["action"] = "OBSERVE"
                        item["paper_only"] = True
                        item["allocation_active"] = False
                        item["canary_armed"] = False
                        prepared_members.append(item)
                    try:
                        setter(
                            "rolling_selection_activation",
                            {
                                "selection_id": active_selection.get(
                                    "selection_id",
                                    active_selection.get("portfolio_selection_id"),
                                ),
                                "status": "PREPARED",
                                "paper_only": True,
                                "allocation_active": False,
                                "canary_armed": False,
                                "k": 0,
                                "members": prepared_members,
                                "allocation_activation": {
                                    "status": "PREPARED",
                                    "rolled_back_at": utc_now().isoformat(),
                                },
                            },
                        )
                    except Exception as exc:
                        rollback_failures.append(
                            f"SELECTION_ROLLBACK_FAILED:{type(exc).__name__}"
                        )
            if service is not None and armed_new:
                try:
                    service.disarm()
                except Exception as exc:
                    rollback_failures.append(f"DISARM_FAILED:{type(exc).__name__}")
            if activated_new and auth_id:
                try:
                    self.revoke_execution_authorization(
                        auth_id,
                        actor=actor,
                        expected_generation=activated_generation,
                        reason="exploratory_live_review_rollback",
                    )
                except Exception as exc:
                    rollback_failures.append(f"REVOKE_FAILED:{type(exc).__name__}")
            if rollback_failures:
                failsafe = service or getattr(self, "_canary_service", None)
                if failsafe is not None:
                    try:
                        failsafe.kill()
                    except Exception as exc:
                        rollback_failures.append(f"KILL_FAILED:{type(exc).__name__}")
                failure_document = dict(auth)
                failure_document.update(
                    {
                        "status": "UNKNOWN",
                        "rollback_status": "INCOMPLETE",
                        "rollback_failures": list(rollback_failures),
                        "paper_only": True,
                        "live_execution": False,
                    }
                )
                try:
                    self.store.set_operator_config(
                        "execution_authorization_review", failure_document
                    )
                except Exception as exc:
                    rollback_failures.append(
                        f"ROLLBACK_STATUS_PERSIST_FAILED:{type(exc).__name__}"
                    )
                self._audit(
                    "exploratory.live.rollback_failed",
                    auth_id or "unknown",
                    success=False,
                    reason=";".join(rollback_failures),
                    result={"rollback_failures": rollback_failures},
                )
                raise OperatorControlError(
                    "EXPLORATORY_LIVE_ROLLBACK_INCOMPLETE"
                ) from original_error
            raise
    def controller_lease_status(self) -> dict[str, Any]:
        loader = getattr(self.store, "load_canary_controller_lease", None)
        if not callable(loader):
            return {"status": "UNAVAILABLE", "owner_id": None, "paper_only": True}
        try:
            value = loader(now=utc_now())
        except TypeError:
            value = loader()
        except Exception:
            value = None
        projected = _safe_value(value) if isinstance(value, Mapping) else {}
        result = dict(projected) if isinstance(projected, Mapping) else {}
        result.setdefault("status", "NONE")
        result["paper_only"] = True
        result["live_execution"] = False
        return result


    def rolling_portfolio_state(self) -> dict[str, Any]:
        """Return persisted rolling state without mutating read/status paths."""
        state_loader = getattr(self.store, "load_portfolio_review_state", None)
        state = state_loader() if callable(state_loader) else None
        result = dict(state) if isinstance(state, Mapping) else {}
        selection_loader = getattr(self.store, "load_current_portfolio_selection", None)
        loaded_selection = selection_loader() if callable(selection_loader) else None
        selection = (
            dict(loaded_selection)
            if isinstance(loaded_selection, Mapping)
            else (
                dict(result.get("selection"))
                if isinstance(result.get("selection"), Mapping)
                else {}
            )
        )
        if selection:
            result["selection"] = dict(selection)
            result.setdefault(
                "portfolio_selection_id",
                selection.get("portfolio_selection_id", selection.get("selection_id")),
            )
            members_value = selection.get("members", selection.get("selected_members", ()))
            members = (
                list(members_value)
                if isinstance(members_value, (list, tuple))
                else []
            )
            result.setdefault("k", selection.get("k", len(members)))
            result.setdefault("actual", len(members))
        else:
            members = []

        get_config = getattr(self.store, "get_operator_config", None)
        active = get_config("rolling_admission_policy_active", {}) if callable(get_config) else {}
        active = dict(active) if isinstance(active, Mapping) else {}
        if active:
            result["active_policy"] = dict(active)
            result.setdefault("policy", dict(active))
            result.setdefault(
                "risk",
                {
                    key: active.get(key)
                    for key in (
                        "risk_config_id",
                        "risk_config_generation",
                        "risk_config_hash",
                    )
                    if active.get(key) is not None
                },
            )
        reviewed = get_config("rolling_admission_policy_review", {}) if callable(get_config) else {}
        for label, value in (("active", active), ("reviewed", reviewed)):
            if not isinstance(value, Mapping):
                continue
            try:
                identity = _rolling_policy_identity(value)
            except OperatorControlError as exc:
                identity = {
                    "status": "INVALID",
                    "blocker": exc.code,
                }
            else:
                identity.update(
                    {
                        "reviewed_at": value.get("reviewed_at"),
                        "active_at": value.get("active_at"),
                        "status": value.get("status") or value.get("review_status"),
                    }
                )
            result[f"{label}_policy_identity"] = identity
            result[f"{label}_policy"] = dict(value)

        # ``actual_k`` remains the persisted-member count.  Actionability is a
        # separate, fail-closed projection of the runtime funded-member gate.
        result.setdefault("actual_k", result.get("actual", 0))
        evidence_loader = getattr(self.store, "list_strategy_evidence_windows", None)
        actionable_count = 0
        blocker_rows: list[dict[str, Any]] = []
        reason_counts: dict[str, int] = {}
        for index, raw_member in enumerate(members[:64]):
            if not isinstance(raw_member, Mapping):
                reasons = ["MEMBER_MAPPING_REQUIRED"]
                member_id = ""
                status = ""
                actionable = False
            else:
                actionable, reasons = _rolling_actionability(
                    raw_member,
                    selection,
                    active,
                    evidence_loader if callable(evidence_loader) else None,
                )
                member_id = _rolling_identity_value(raw_member, "strategy_version_id")[:256]
                status = _rolling_text(raw_member.get("status")).upper()[:32]
            if actionable:
                actionable_count += 1
                continue
            for reason in reasons:
                if len(reason_counts) >= 32 and reason not in reason_counts:
                    continue
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            if len(blocker_rows) < 32:
                blocker_rows.append(
                    {
                        "index": index,
                        "strategy_version_id": member_id,
                        "status": status,
                        "blockers": reasons[:16],
                    }
                )
        result["actionable"] = actionable_count
        result["actionable_blockers"] = blocker_rows
        result["actionable_reasons"] = reason_counts
        result.setdefault(
            "controller_status",
            str(result.get("status") or ("CURRENT" if selection else "COLD_START")).upper(),
        )
        result.setdefault("paper_only", True)
        result.setdefault("live_execution", False)
        result.setdefault("execution_authority", False)

        return result
    def _rolling_risk_binding(self) -> dict[str, Any]:
        snapshot = self.risk_settings_snapshot()
        active = snapshot.get("active") if isinstance(snapshot, Mapping) else {}
        active = active if isinstance(active, Mapping) else {}
        config_id = str(
            snapshot.get("config_id") or active.get("config_id") or ""
        ).strip()
        config_hash = str(
            snapshot.get("config_hash") or active.get("config_hash") or ""
        ).strip()
        try:
            generation = int(snapshot.get("generation") or active.get("generation") or 0)
        except (TypeError, ValueError):
            generation = 0
        if not config_id or not config_hash or generation <= 0:
            raise OperatorControlError("ACTIVE_RISK_CONFIG_REQUIRED")
        return {
            "risk_config_id": config_id,
            "risk_config_generation": generation,
            "risk_config_hash": config_hash,
            "active_risk_config_id": config_id,
            "active_risk_config_generation": generation,
            "active_risk_config_hash": config_hash,
        }

    def _rolling_active_policy_document(self) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return the active envelope and its exact immutable policy document."""
        loader = getattr(self.store, "get_operator_config", None)
        missing = object()
        active_raw = loader("rolling_admission_policy_active", missing) if callable(loader) else missing
        if active_raw is missing:
            return {}, {}
        if not isinstance(active_raw, Mapping):
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_REQUIRED")
        active = dict(active_raw)
        if not active:
            return {}, {}
        identity_source = dict(active)
        try:
            expected_identity = _rolling_policy_identity(identity_source)
        except OperatorControlError as exc:
            raise OperatorControlError(
                "ROLLING_POLICY_IMMUTABLE_REQUIRED", str(exc)
            ) from exc
        policy_loader = getattr(self.store, "load_admission_policy", None)
        if not callable(policy_loader):
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_UNAVAILABLE")
        try:
            loaded = policy_loader(
                expected_identity["policy_id"],
                expected_identity["version"],
            )
        except Exception as exc:
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_LOAD_FAILED") from exc
        if not isinstance(loaded, Mapping):
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_NOT_FOUND")
        try:
            persisted_identity = _rolling_policy_identity(loaded)
        except OperatorControlError as exc:
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_INVALID", str(exc)) from exc
        if persisted_identity != expected_identity:
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_MISMATCH")
        return active, dict(loaded)

    @staticmethod
    def _rolling_canonical_hash(value: Mapping[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(
                _safe_value(value),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                default=str,
            ).encode("utf-8")
        ).hexdigest()
    @staticmethod
    def _rolling_fence_hash(value: Mapping[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                default=str,
            ).encode("utf-8")
        ).hexdigest()

    def _rolling_effective_limits(self) -> dict[str, Any]:
        """Expose the exact active canary limits used for draft feasibility."""
        snapshot = self.risk_settings_snapshot()
        effective = snapshot.get("effective_limits") if isinstance(snapshot, Mapping) else {}
        if not isinstance(effective, Mapping):
            effective = snapshot.get("active_limits", {}) if isinstance(snapshot, Mapping) else {}
        if not isinstance(effective, Mapping):
            raise OperatorControlError("ACTIVE_RISK_CONFIG_REQUIRED")

        def first(*names: str) -> Any:
            for name in names:
                value = effective.get(name)
                if value is not None and value != "":
                    return value
            return None

        result = {
            "max_submitted_orders_per_day": first(
                "max_submitted_orders_per_day", "max_orders_per_day"
            ),
            "max_all_in_buy_usd": first("max_all_in_buy_usd", "target_notional_usd"),
            "max_fee_reserve_usd": first(
                "max_fee_reserve_usd", "max_buy_fee_reserve_usd", "buy_fee_reserve_usd"
            ),
            "max_gross_daily_buy_usd": first(
                "max_gross_daily_buy_usd", "gross_daily_buy_usd"
            ),
            "max_aggregate_open_cost_usd": first(
                "max_aggregate_open_cost_usd", "aggregate_open_cost_usd"
            ),
            "max_aggregate_exposure_usd": first(
                "max_aggregate_exposure_usd", "max_exposure_usd", "aggregate_exposure_usd"
            ),
            "max_positions": first("max_positions", "max_open_positions"),
            "per_market_buy_cap_usd": first("per_market_buy_cap_usd"),
            "per_event_buy_cap_usd": first("per_event_buy_cap_usd"),
            "cumulative_buy_cap_usd": first("cumulative_buy_cap_usd"),
            "realized_loss_entry_stop_usd": first(
                "realized_loss_entry_stop_usd", "max_daily_loss_usd"
            ),
            "equity_loss_entry_stop_usd": first("equity_loss_entry_stop_usd"),
            "max_slippage_bps": first("max_slippage_bps"),
        }
        required = (
            "max_submitted_orders_per_day",
            "max_all_in_buy_usd",
            "max_gross_daily_buy_usd",
            "max_aggregate_open_cost_usd",
            "max_aggregate_exposure_usd",
            "max_positions",
        )
        if any(result[name] is None for name in required):
            raise OperatorControlError("ACTIVE_RISK_LIMITS_UNAVAILABLE")
        return result

    @staticmethod
    def _rolling_budget_cap(limits: Mapping[str, Any]) -> Decimal:
        """Compute a single portfolio budget cap from independent active fences."""
        try:
            all_in = Decimal(str(limits["max_all_in_buy_usd"]))
            gross = Decimal(str(limits["max_gross_daily_buy_usd"]))
            exposure = Decimal(str(limits["max_aggregate_exposure_usd"]))
            open_positions = int(limits["max_positions"])
            submissions = int(limits["max_submitted_orders_per_day"])
        except (ArithmeticError, TypeError, ValueError, KeyError) as exc:
            raise OperatorControlError("ACTIVE_RISK_LIMITS_INVALID") from exc
        if (
            all_in < 0
            or gross < 0
            or exposure < 0
            or open_positions < 0
            or submissions < 0
        ):
            raise OperatorControlError("ACTIVE_RISK_LIMITS_INVALID")
        candidates = [gross, exposure, all_in * open_positions, all_in * submissions]
        for field in (
            "max_aggregate_open_cost_usd",
            "per_market_buy_cap_usd",
            "per_event_buy_cap_usd",
            "cumulative_buy_cap_usd",
        ):
            value = limits.get(field)
            if value not in (None, ""):
                try:
                    candidates.append(Decimal(str(value)))
                except (ArithmeticError, TypeError, ValueError) as exc:
                    raise OperatorControlError("ACTIVE_RISK_LIMITS_INVALID") from exc
        cap = min(candidates, default=Decimal("0"))
        if not cap.is_finite() or cap < 0:
            raise OperatorControlError("ACTIVE_RISK_LIMITS_INVALID")
        return cap

    def _rolling_venue_feasibility(
        self,
        *,
        all_in_buy: Decimal,
    ) -> dict[str, Any]:
        """Project persisted venue-minimum evidence without performing transport."""
        getter = getattr(self.store, "get_operator_config", None)
        connectivity = getter(CANARY_CONNECTIVITY_CONFIG_KEY, {}) if callable(getter) else {}
        failures: list[str] = []
        if isinstance(connectivity, Mapping):
            failure_values = connectivity.get("failure_codes", connectivity.get("failures", ()))
            if isinstance(failure_values, str):
                failure_values = (failure_values,)
            if isinstance(failure_values, (list, tuple, set, frozenset)):
                for value in failure_values:
                    code = str(value).strip().upper()
                    if code in _CONNECTIVITY_FAILURE_CODES and code not in failures:
                        failures.append(code)
            diagnostics = connectivity.get("diagnostics")
            if isinstance(diagnostics, Mapping):
                for key in ("market", "order_book", "venue"):
                    details = diagnostics.get(key)
                    if not isinstance(details, Mapping):
                        continue
                    nested_failures = details.get("failure_codes", details.get("failures", ()))
                    if isinstance(nested_failures, str):
                        nested_failures = (nested_failures,)
                    if isinstance(nested_failures, (list, tuple, set, frozenset)):
                        for value in nested_failures:
                            code = str(value).strip().upper()
                            if code in _CONNECTIVITY_FAILURE_CODES and code not in failures:
                                failures.append(code)
        minimum: Decimal | None = None
        if isinstance(connectivity, Mapping):
            for key in (
                "venue_minimum_notional_usd",
                "venue_minimum_notional",
                "minimum_notional_usd",
                "minimum_notional",
                "min_notional",
            ):
                raw_minimum = connectivity.get(key)
                try:
                    candidate = Decimal(str(raw_minimum))
                except (ArithmeticError, TypeError, ValueError):
                    continue
                if candidate.is_finite() and candidate >= 0:
                    minimum = candidate
                    break
            diagnostics = connectivity.get("diagnostics")
            if minimum is None and isinstance(diagnostics, Mapping):
                for key in ("market", "order_book", "venue"):
                    details = diagnostics.get(key)
                    if not isinstance(details, Mapping):
                        continue
                    for field in (
                        "venue_minimum_notional_usd",
                        "venue_minimum_notional",
                        "minimum_notional_usd",
                        "minimum_notional",
                        "min_notional",
                    ):
                        raw_minimum = details.get(field)
                        try:
                            candidate = Decimal(str(raw_minimum))
                        except (ArithmeticError, TypeError, ValueError):
                            continue
                        if candidate.is_finite() and candidate >= 0:
                            minimum = candidate
                            break
                    if minimum is not None:
                        break
        if "VENUE_MINIMUM_EXCEEDS_CANARY_TARGET" in failures:
            feasible: bool | None = False
            status = "BLOCKED"
        elif minimum is None:
            feasible = None
            status = "UNKNOWN"
        else:
            feasible = minimum <= all_in_buy
            status = "FEASIBLE" if feasible else "BLOCKED"
        return {
            "status": status,
            "feasible": feasible,
            "minimum_notional_usd": (
                format(minimum, "f") if minimum is not None else None
            ),
            "checked_at": connectivity.get("checked_at")
            if isinstance(connectivity, Mapping)
            else None,
            "failure_codes": failures[:16],
        }

    def review_rolling_admission_policy(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        actor: str = "operator",
        expected_risk_config_id: Any | None = None,
        expected_risk_config_generation: Any | None = None,
        expected_risk_config_hash: Any | None = None,
    ) -> dict[str, Any]:
        """Create a non-active, internally identified rolling allocation draft."""
        if values is not None and not isinstance(values, Mapping):
            raise OperatorControlError("ROLLING_POLICY_REQUIRED")
        actor_value = _safe_identifier(actor, "actor")
        binding = self._rolling_risk_binding()
        expected_binding = {
            "risk_config_id": expected_risk_config_id,
            "risk_config_generation": expected_risk_config_generation,
            "risk_config_hash": expected_risk_config_hash,
        }
        for name, expected in expected_binding.items():
            if expected is not None and str(expected).strip() != str(binding[name]):
                raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")

        active_envelope, active_document = self._rolling_active_policy_document()
        raw = dict(values or {})
        if _rolling_identity_present(raw):
            try:
                input_identity = _rolling_policy_identity(raw)
            except OperatorControlError as exc:
                raise OperatorControlError(
                    "ROLLING_POLICY_IDENTITY_INVALID", str(exc)
                ) from exc
        else:
            input_identity = {}
        nested = raw.get("policy")
        if isinstance(nested, RollingAdmissionPolicy):
            nested = nested.as_dict()
        nested = dict(nested) if isinstance(nested, Mapping) else {}
        policy_input = dict(active_document) if active_document else {}
        policy_input.update({key: value for key, value in raw.items() if key != "policy"})
        policy_input.update(nested)
        policy_input.pop("policy", None)
        if input_identity:
            policy_input["policy_id"] = input_identity["policy_id"]
            policy_input["version"] = input_identity["version"]
            policy_input["config_hash"] = input_identity["config_hash"]
        else:
            # Values-only reviews are application-owned immutable documents.  Do
            # not reuse the built-in default identity (or a prior active
            # identity), even before the first activation.  The final identity
            # is derived below, after policy validation and budget clamping.
            for alias in (
                *_ROLLING_POLICY_ID_ALIASES,
                *_ROLLING_POLICY_VERSION_ALIASES,
                *_ROLLING_POLICY_HASH_ALIASES,
            ):
                policy_input.pop(alias, None)
            policy_input["policy_id"] = "rolling-values"
            policy_input["version"] = "rolling-policy-values-v0"
            policy_input.pop("config_hash", None)
        try:
            policy = RollingAdmissionPolicy.from_mapping(policy_input)
        except (TypeError, ValueError) as exc:
            raise OperatorControlError("ROLLING_POLICY_INVALID", str(exc)) from exc

        limits = self._rolling_effective_limits()
        budget_cap = self._rolling_budget_cap(limits)
        requested_budget = _rolling_decimal(policy.global_budget)
        if requested_budget is None:
            raise OperatorControlError("ROLLING_POLICY_BUDGET_INVALID")
        if requested_budget > budget_cap:
            if values is None or (
                "global_budget" not in raw and "global_budget" not in nested
            ):
                requested_budget = budget_cap
            else:
                raise OperatorControlError("ROLLING_POLICY_BUDGET_EXCEEDS_ACTIVE_CAP")
        # Canonicalize the one portfolio-wide budget in the immutable policy row.
        policy_input["global_budget"] = format(requested_budget, "f")
        try:
            policy = RollingAdmissionPolicy.from_mapping(policy_input)
        except (TypeError, ValueError) as exc:
            raise OperatorControlError("ROLLING_POLICY_INVALID", str(exc)) from exc
        if not input_identity:
            canonical_values = policy.as_dict()
            for alias in (
                *_ROLLING_POLICY_ID_ALIASES,
                *_ROLLING_POLICY_VERSION_ALIASES,
                *_ROLLING_POLICY_HASH_ALIASES,
            ):
                canonical_values.pop(alias, None)
            identity_digest = hashlib.sha256(
                json.dumps(
                    _safe_value(
                        {
                            "policy": canonical_values,
                            "risk_binding": binding,
                        }
                    ),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    default=str,
                ).encode("utf-8")
            ).hexdigest()
            policy_input["policy_id"] = "rolling-policy:" + identity_digest[:32]
            policy_input["version"] = "policy-" + identity_digest[32:]
            policy_input.pop("config_hash", None)
            try:
                policy = RollingAdmissionPolicy.from_mapping(policy_input)
            except (TypeError, ValueError) as exc:
                raise OperatorControlError("ROLLING_POLICY_INVALID", str(exc)) from exc

        active_identity: dict[str, str] = {}
        if active_envelope:
            try:
                active_identity = _rolling_policy_identity(active_envelope)
            except OperatorControlError:
                active_identity = {}
        selected = (
            self.store.load_current_portfolio_selection()
            if callable(getattr(self.store, "load_current_portfolio_selection", None))
            else None
        )
        selected_id = (
            _rolling_text(selected.get("portfolio_selection_id") or selected.get("selection_id"))
            if isinstance(selected, Mapping)
            else ""
        )
        allocation = {
            "global_budget_usd": format(policy.global_budget, "f"),
            "budget_cap_usd": format(budget_cap, "f"),
            "within_active_caps": True,
            "max_active_strategies": policy.max_members,
            "max_open_positions": int(limits["max_positions"]),
            "active_submissions_per_day": int(limits["max_submitted_orders_per_day"]),
            "active_per_buy_usd": str(limits["max_all_in_buy_usd"]),
            "active_daily_buy_usd": str(limits["max_gross_daily_buy_usd"]),
            "active_open_exposure_usd": str(limits["max_aggregate_exposure_usd"]),
            "active_per_market_buy_usd": limits["per_market_buy_cap_usd"],
            "active_per_event_buy_usd": limits["per_event_buy_cap_usd"],
            "active_cumulative_buy_usd": limits["cumulative_buy_cap_usd"],
            "venue_minimum_feasibility": self._rolling_venue_feasibility(
                all_in_buy=Decimal(str(limits["max_all_in_buy_usd"]))
            ),
            "selection_id": selected_id or None,
        }
        draft_id = "rolling-draft:" + uuid.uuid4().hex
        draft_version = "draft-" + uuid.uuid4().hex[:16]
        reviewed_at = utc_now().isoformat()
        policy_document = policy.as_dict()
        draft_hash = self._rolling_canonical_hash(
            {
                "draft_id": draft_id,
                "draft_version": draft_version,
                "policy": policy_document,
                "binding": binding,
                "allocation": allocation,
            }
        )
        document = {
            **policy_document,
            **binding,
            "policy": policy_document,
            "draft_id": draft_id,
            "draft_version": draft_version,
            "draft_hash": draft_hash,
            "allocation_review": allocation,
            "reviewed_active_policy_id": active_identity.get("policy_id"),
            "reviewed_active_policy_version": active_identity.get("version"),
            "reviewed_active_policy_hash": active_identity.get("config_hash"),
            "reviewed_selection_id": selected_id or None,
            "reviewed_at": reviewed_at,
            "reviewed_by": actor_value,
            "review_status": "REVIEWED",
            "status": "REVIEWED",
            "paper_only": True,
            "live_execution": False,
        }
        # The policy row is append-only.  The operator pointer may move to the
        # newest draft, but prior immutable identities remain addressable.
        self.store.save_admission_policy(policy_document)
        self.store.set_operator_config("rolling_admission_policy_review", document)
        draft_projection = _safe_value(document)
        if isinstance(draft_projection, Mapping):
            draft_projection = dict(draft_projection)
            draft_projection.update({"paper_only": True, "live_execution": False})
        return {
            "policy": _safe_value(document),
            "draft": draft_projection,
            "allocation_review": _safe_value(allocation),
            "review": _safe_value(self.rolling_portfolio_state()),
            "status": "REVIEWED",
            "paper_only": True,
            "live_execution": False,
        }

    def activate_rolling_admission_policy(
        self,
        policy_id: Any = None,
        policy_version: Any = None,
        *,
        actor: str = "operator",
        draft_id: Any | None = None,
        draft_version: Any | None = None,
        expected_risk_config_id: Any | None = None,
        expected_risk_config_generation: Any | None = None,
        expected_risk_config_hash: Any | None = None,
    ) -> dict[str, Any]:
        """Activate a reviewed policy only after its exact selection is committed."""
        actor_value = _safe_identifier(actor, "actor")
        reviewed = self.store.get_operator_config("rolling_admission_policy_review", {})
        if not isinstance(reviewed, Mapping):
            raise OperatorControlError("ROLLING_POLICY_REVIEW_REQUIRED")
        reviewed = dict(reviewed)
        try:
            reviewed_identity = _rolling_policy_identity(reviewed)
        except OperatorControlError as exc:
            raise OperatorControlError("ROLLING_POLICY_REVIEW_INVALID", str(exc)) from exc
        reviewed_draft_id = _rolling_text(reviewed.get("draft_id"))
        reviewed_draft_version = _rolling_text(reviewed.get("draft_version"))
        if (policy_id is None) != (policy_version is None):
            raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
        if policy_id is not None and (
            not str(policy_id).strip() or not str(policy_version).strip()
        ):
            raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
        if (draft_id is None) != (draft_version is None):
            raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
        if draft_id is not None and (
            not str(draft_id).strip() or not str(draft_version).strip()
        ):
            raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
        if draft_id is not None and _safe_identifier(draft_id, "rolling draft ID") != reviewed_draft_id:
            raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
        if draft_version is not None and _safe_identifier(draft_version, "rolling draft version") != reviewed_draft_version:
            raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
        policy_value = (
            _safe_identifier(policy_id, "policy ID")
            if policy_id is not None and str(policy_id).strip()
            else reviewed_identity["policy_id"]
        )
        version_value = (
            _safe_identifier(policy_version, "policy version")
            if policy_version is not None and str(policy_version).strip()
            else reviewed_identity["version"]
        )
        if (
            reviewed_identity["policy_id"] != policy_value
            or reviewed_identity["version"] != version_value
            or str(reviewed.get("status") or reviewed.get("review_status") or "").upper() != "REVIEWED"
            or not reviewed_draft_id
            or not reviewed_draft_version
        ):
            raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
        binding = self._rolling_risk_binding()
        for name, expected in (
            ("risk_config_id", expected_risk_config_id),
            ("risk_config_generation", expected_risk_config_generation),
            ("risk_config_hash", expected_risk_config_hash),
        ):
            if expected is not None and str(expected).strip() != str(binding[name]):
                raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
        for key in binding:
            if str(reviewed.get(key, "")).strip() != str(binding.get(key, "")).strip():
                raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")

        active_envelope, _ = self._rolling_active_policy_document()
        for key, expected in (
            ("reviewed_active_policy_id", "policy_id"),
            ("reviewed_active_policy_version", "version"),
            ("reviewed_active_policy_hash", "config_hash"),
        ):
            recorded = _rolling_text(reviewed.get(key))
            current = ""
            if active_envelope:
                try:
                    current = _rolling_policy_identity(active_envelope).get(expected, "")
                except OperatorControlError:
                    current = ""
            if recorded != current:
                raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")

        policy_loader = getattr(self.store, "load_admission_policy", None)
        if not callable(policy_loader):
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_UNAVAILABLE")
        try:
            policy = policy_loader(policy_value, version_value)
        except Exception as exc:
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_LOAD_FAILED") from exc
        if not isinstance(policy, Mapping):
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_NOT_FOUND")
        try:
            persisted_identity = _rolling_policy_identity(policy)
        except OperatorControlError as exc:
            raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_INVALID", str(exc)) from exc
        if persisted_identity != reviewed_identity:
            raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
        reviewed_at = _rolling_text(reviewed.get("reviewed_at"))
        if not reviewed_at:
            raise OperatorControlError("ROLLING_POLICY_REVIEW_INVALID")
        allocation = reviewed.get("allocation_review")
        if not isinstance(allocation, Mapping) or allocation.get("within_active_caps") is not True:
            raise OperatorControlError("ROLLING_POLICY_REVIEW_INVALID")

        reviewed_hash = self._rolling_fence_hash(reviewed)
        active_snapshot = dict(active_envelope)
        transaction = getattr(self.store, "transaction", None)
        if not callable(transaction):
            raise OperatorControlError("ROLLING_POLICY_TRANSACTION_UNAVAILABLE")
        try:
            with transaction(immediate=True):
                current_review = self.store.get_operator_config(
                    "rolling_admission_policy_review",
                    {},
                )
                if (
                    not isinstance(current_review, Mapping)
                    or self._rolling_fence_hash(dict(current_review)) != reviewed_hash
                ):
                    raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
                current_active_raw = self.store.get_operator_config(
                    "rolling_admission_policy_active",
                    None,
                )
                current_active = (
                    dict(current_active_raw)
                    if isinstance(current_active_raw, Mapping)
                    else {}
                )
                if self._rolling_fence_hash(current_active) != self._rolling_fence_hash(active_snapshot):
                    raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
                current_binding = self._rolling_risk_binding()
                if any(
                    str(current_binding.get(key, "")) != str(binding.get(key, ""))
                    for key in binding
                ):
                    raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
                current_policy = policy_loader(policy_value, version_value)
                if not isinstance(current_policy, Mapping):
                    raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_NOT_FOUND")
                if _rolling_policy_identity(current_policy) != persisted_identity:
                    raise OperatorControlError("ROLLING_POLICY_IMMUTABLE_MISMATCH")
                active_at = utc_now().isoformat()
                active = {
                    "policy": _safe_value(current_policy),
                    "policy_id": persisted_identity["policy_id"],
                    "version": persisted_identity["version"],
                    "policy_version": persisted_identity["version"],
                    "config_hash": persisted_identity["config_hash"],
                    **binding,
                    "draft_id": reviewed_draft_id,
                    "draft_version": reviewed_draft_version,
                    "draft_hash": reviewed.get("draft_hash"),
                    "reviewed_at": reviewed_at,
                    "active_at": active_at,
                    "activated_at": active_at,
                    "activated_by": actor_value,
                    "status": "ACTIVE",
                    "paper_only": True,
                    "live_execution": False,
                }
                cas = getattr(self.store, "compare_and_set_operator_config", None)
                if callable(cas) and active_snapshot:
                    if not cas("rolling_admission_policy_active", active_snapshot, active):
                        raise OperatorControlError("ROLLING_POLICY_REVIEW_STALE")
                else:
                    self.store.set_operator_config("rolling_admission_policy_active", active)
                try:
                    self._research_processor.review_rolling_portfolio(
                        now=utc_now(),
                        force=True,
                    )
                except OperatorControlError:
                    raise
                except Exception as exc:
                    raise OperatorControlError(
                        "ROLLING_POLICY_REVIEW_FAILED",
                        type(exc).__name__,
                    ) from exc
                selection_loader = getattr(self.store, "load_current_portfolio_selection", None)
                selection = selection_loader() if callable(selection_loader) else None
                if not isinstance(selection, Mapping):
                    raise OperatorControlError("ROLLING_POLICY_SELECTION_REQUIRED")
                if (
                    _rolling_text(selection.get("policy_id") or selection.get("admission_policy_id"))
                    != persisted_identity["policy_id"]
                    or _rolling_text(
                        selection.get("policy_version") or selection.get("admission_policy_version")
                    )
                    != persisted_identity["version"]
                ):
                    raise OperatorControlError("ROLLING_POLICY_SELECTION_STALE")
                selected_policy_source = selection.get("policy_config")
                selected_policy_source = (
                    selected_policy_source
                    if isinstance(selected_policy_source, Mapping)
                    else selection
                )
                selected_policy_hash = _rolling_text(
                    selected_policy_source.get("config_hash")
                    or selected_policy_source.get("policy_hash")
                )
                if selected_policy_hash != persisted_identity["config_hash"]:
                    raise OperatorControlError("ROLLING_POLICY_SELECTION_STALE")
                selected_policy = policy_loader(
                    persisted_identity["policy_id"],
                    persisted_identity["version"],
                )
                if (
                    not isinstance(selected_policy, Mapping)
                    or _rolling_policy_identity(selected_policy) != persisted_identity
                ):
                    raise OperatorControlError("ROLLING_POLICY_SELECTION_STALE")
                job_saver = getattr(self.store, "set_operator_job", None)
                if callable(job_saver):
                    try:
                        job_saver("rolling_admission_policy_active", "ACTIVE", active, resumable=True)
                    except TypeError:
                        job_saver("rolling_admission_policy_active", "ACTIVE", active)
        except OperatorControlError:
            raise
        except Exception as exc:
            raise OperatorControlError("ROLLING_POLICY_ACTIVATION_FAILED", type(exc).__name__) from exc
        return {
            "policy": _safe_value(policy),
            "active": _safe_value(active),
            "draft_id": reviewed_draft_id,
            "draft_version": reviewed_draft_version,
            "status": "ACTIVE",
            "paper_only": True,
            "live_execution": False,
        }


    def _cached_pid_matches_node(self, pid: int) -> bool:
        """Bound the Windows CIM/process identity work used by status reads."""
        try:
            pid_value = int(pid)
        except (TypeError, ValueError):
            return False
        if pid_value <= 0:
            return False
        try:
            marker_lines = Path(self.db_path + ".node.pid").read_text(
                encoding="ascii"
            ).splitlines()
            if len(marker_lines) < 2 or int(marker_lines[0].strip()) != pid_value:
                return False
            marker_start_ticks = int(marker_lines[1].strip())
        except (FileNotFoundError, OSError, UnicodeError, ValueError):
            return False
        now = time.monotonic()
        key = (pid_value, self.db_path, marker_start_ticks)
        cached = self._pid_identity_cache.get(key)
        if cached is not None and now - cached[0] <= self._pid_identity_cache_ttl:
            return cached[1]
        result = bool(
            _pid_matches_node(
                pid_value,
                self.db_path,
                expected_start_ticks=marker_start_ticks,
            )
        )
        self._pid_identity_cache[key] = (now, result)
        if len(self._pid_identity_cache) > self._pid_identity_cache_limit:
            oldest = min(self._pid_identity_cache, key=self._pid_identity_cache.__getitem__)
            self._pid_identity_cache.pop(oldest, None)
        return result

    def _begin_action(self, action: str, target: str) -> tuple[str, dict[str, Any] | None]:
        """Persist an in-flight action before any side effect is dispatched."""
        with self._action_lock:
            raw = self.store.get_operator_config("operator_action_state", {})
            body = dict(raw) if isinstance(raw, Mapping) else {}
            entries = body.get("actions")
            entries = [dict(item) for item in entries if isinstance(item, Mapping)] if isinstance(entries, list) else []
            for entry in entries:
                if (
                    str(entry.get("action")) == action
                    and str(entry.get("target")) == target
                    and str(entry.get("status")).upper() == "RUNNING"
                ):
                    return str(entry.get("action_id") or ""), entry
            action_id = "operator-action:" + uuid.uuid4().hex
            entry = {
                "action_id": action_id,
                "action": action,
                "target": target,
                "status": "RUNNING",
                "started_at": utc_now().isoformat(),
                "pid": os.getpid(),
            }
            entries.append(entry)
            body["actions"] = entries[-32:]
            self.store.set_operator_config("operator_action_state", body)
            return action_id, None

    def _finish_action(
        self,
        action_id: str | None,
        *,
        status: str,
        result: Mapping[str, Any] | None = None,
        reason: str = "",
    ) -> None:
        if not action_id:
            return
        try:
            with self._action_lock:
                raw = self.store.get_operator_config("operator_action_state", {})
                body = dict(raw) if isinstance(raw, Mapping) else {}
                entries = body.get("actions")
                if not isinstance(entries, list):
                    return
                updated: list[dict[str, Any]] = []
                for raw_entry in entries:
                    if not isinstance(raw_entry, Mapping):
                        continue
                    entry = dict(raw_entry)
                    if str(entry.get("action_id")) == action_id:
                        entry.update(
                            {
                                "status": status,
                                "completed_at": utc_now().isoformat(),
                                "reason": reason[:160],
                                "result": _safe_value(result or {}),
                            }
                        )
                    updated.append(entry)
                body["actions"] = updated[-32:]
                self.store.set_operator_config("operator_action_state", body)
        except Exception:
            return

    def _action_snapshot(self) -> list[dict[str, Any]]:
        raw = self.store.get_operator_config("operator_action_state", {})
        entries = raw.get("actions") if isinstance(raw, Mapping) else []
        if not isinstance(entries, list):
            return []
        return [
            dict(item)
            for item in entries[-32:]
            if isinstance(item, Mapping)
        ]

    def configure_hermes_job_id(self, job_id: str) -> str:
        value = _safe_identifier(job_id, "Hermes job ID")
        self.store.set_operator_config("hermes_research_job_id", value)
        self.hermes_job_id = value
        return value

    def _node_lock_path(self) -> Path:
        return Path(str(self.db_path) + ".lock")

    def _node_status(self, *, verify_identity: bool = True) -> dict[str, Any]:
        workers = self.store.list_worker_states(limit=32)
        root = next((item for item in workers if str(item.get("worker_name")) == "axiom-node"), {})
        worker_payload = root.get("payload") if isinstance(root.get("payload"), Mapping) else {}
        configured_lock = worker_payload.get("lock_path")
        lock_path = (
            Path(str(configured_lock))
            if isinstance(configured_lock, str) and configured_lock.strip()
            else self._node_lock_path()
        )
        lock_pid = 0
        lock_exists: bool | None = None
        if verify_identity:
            lock_text = ""
            try:
                lock_text = lock_path.read_text(encoding="ascii")
                lock_pid = int(lock_text.splitlines()[0].strip())
            except (FileNotFoundError, OSError, ValueError):
                pass
            lock_exists = lock_path.exists()
        else:
            persisted_lock_exists = worker_payload.get("lock_exists")
            if isinstance(persisted_lock_exists, bool):
                lock_exists = persisted_lock_exists
        persisted_pid = worker_payload.get("pid")
        try:
            pid = int(lock_pid or persisted_pid or 0)
        except (TypeError, ValueError):
            pid = 0
        persisted_status = str(root.get("status") or "").lower()
        heartbeat = root.get("heartbeat_at")
        heartbeat_age: float | None = None
        if heartbeat:
            try:
                stamp = datetime.fromisoformat(str(heartbeat).replace("Z", "+00:00"))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                heartbeat_age = max(
                    0.0,
                    (datetime.now(timezone.utc) - stamp.astimezone(timezone.utc)).total_seconds(),
                )
            except (TypeError, ValueError, OverflowError):
                heartbeat_age = None
        try:
            stale_after = float(worker_payload.get("stale_after_seconds", 300.0))
        except (TypeError, ValueError):
            stale_after = 300.0
        if stale_after <= 0 or stale_after != stale_after:
            stale_after = 300.0
        if verify_identity:
            alive: bool | None = _pid_alive(pid) if pid else False
            if alive is True:
                identity_valid: bool | None = self._cached_pid_matches_node(pid)
            elif alive is None:
                identity_valid = None
            else:
                identity_valid = False
            if alive is True and identity_valid is True:
                state = "RUNNING"
            elif alive is None or identity_valid is None or (alive is True and not identity_valid):
                state = "UNKNOWN"
            elif persisted_status in {"running", "degraded"} and (pid or lock_path.exists()):
                state = "STALE"
            else:
                state = "STOPPED"
        else:
            identity_value = worker_payload.get("worker_identity_valid")
            identity_valid = identity_value if isinstance(identity_value, bool) else None
            alive = None
            if persisted_status in {"running", "degraded"}:
                state = (
                    "STALE"
                    if heartbeat_age is None or heartbeat_age > stale_after
                    else "UNKNOWN"
                )
            elif persisted_status in {"stopped", "closed"}:
                state = "STOPPED"
            elif persisted_status:
                state = persisted_status.upper()
            else:
                state = "UNKNOWN"
        return {
            "status": state,
            "pid": pid or None,
            "started_at": root.get("started_at"),
            "process_identity": worker_payload.get("process_identity"),
            "heartbeat_at": heartbeat,
            "heartbeat_age_seconds": heartbeat_age,
            "stale_after_seconds": stale_after,
            "lock_path": str(lock_path),
            "lock_exists": lock_exists,
            "worker_status": root.get("status"),
            "worker_alive": alive,
            "worker_identity_valid": identity_valid,
            "revision": worker_payload.get("revision"),
            "execution_profile": worker_payload.get("execution_profile"),
        }

    def _clear_stale_lock(self) -> None:
        path = self._node_lock_path()
        try:
            text = path.read_text(encoding="ascii")
            pid = int(text.splitlines()[0].strip())
        except FileNotFoundError:
            return
        except (IndexError, OSError, ValueError):
            raise OperatorControlError("NODE_STATUS_UNKNOWN") from None
        liveness = _pid_alive(pid)
        identity_valid = (
            self._cached_pid_matches_node(pid) if liveness is True else False
        )
        if liveness is True:
            if identity_valid:
                raise OperatorControlError("NODE_ALREADY_RUNNING")
            raise OperatorControlError("NODE_STATUS_UNKNOWN")
        if liveness is None:
            raise OperatorControlError("NODE_STATUS_UNKNOWN")
        raise OperatorControlError("NODE_STALE_LOCK_MANUAL_RECOVERY")

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

    def _require_node_profile(self, status: Mapping[str, Any]) -> None:
        expected_profile = self.execution_profile
        try:
            persisted_raw = status.get("execution_profile")
            persisted_profile = (
                None
                if persisted_raw is None or persisted_raw == ""
                else normalized_execution_profile(persisted_raw)
            )
        except (TypeError, ValueError):
            raise OperatorControlError("NODE_EXECUTION_PROFILE_INVALID") from None
        if (
            expected_profile not in {ISOLATED_EXECUTION_PROFILE, PRODUCTION_EXECUTION_PROFILE}
            or persisted_profile != expected_profile
        ):
            raise OperatorControlError("NODE_EXECUTION_PROFILE_MISMATCH")

    def ensure_node(self, *, wait_seconds: float = 5.0) -> dict[str, Any]:
        if self.execution_profile not in {
            ISOLATED_EXECUTION_PROFILE,
            PRODUCTION_EXECUTION_PROFILE,
        }:
            raise OperatorControlError("INVALID_EXECUTION_PROFILE")
        with self._lock:
            current = self._node_status()

            if current["status"] == "RUNNING":
                self._require_node_profile(current)
                return current
            if current["status"] == "UNKNOWN":
                raise OperatorControlError("NODE_STATUS_UNKNOWN")
            self._clear_stale_lock()
            runtime_db = Path(self.db_path)
            command = [
                sys.executable,
                "-m",
                "axiom.cli",
                "node-run",
                "--db",
                self.db_path,
                "--lock",
                f"{runtime_db}.lock",
                "--log",
                f"{runtime_db}.log",
                "--pid",
                f"{runtime_db}.node.pid",
                "--cycles",
                "0",
            ]
            command.extend(
                [
                    "--historical-refresh-interval",
                    str(self.historical_refresh_interval_seconds),
                    "--historical-refresh-request-budget",
                    str(self.historical_refresh_request_budget),
                    "--historical-refresh-market-budget",
                    str(self.historical_refresh_market_budget),
                ]
            )
            if self.historical_refresh_enabled:
                command.append("--historical-refresh-enabled")
            if self.execution_profile == ISOLATED_EXECUTION_PROFILE:
                command.append("--isolated")
            try:
                process = self._node_launcher(command)
            except (OSError, RuntimeError) as exc:
                raise OperatorControlError("NODE_START_FAILED") from exc
            deadline = time.monotonic() + max(0.1, float(wait_seconds))
            while time.monotonic() < deadline:
                current = self._node_status()
                if current["status"] == "RUNNING":
                    self._require_node_profile(current)
                    return current
                if hasattr(process, "poll") and process.poll() is not None:
                    break
                time.sleep(0.1)
            raise OperatorControlError("NODE_START_TIMEOUT")

    def restart_node(self) -> dict[str, Any]:
        with self._lock:
            current = self._node_status()
            previous_pid = int(current.get("pid") or 0)
            previous_revision = current.get("revision")
            if current["status"] == "RUNNING":
                self._require_node_profile(current)
            if current["status"] == "RUNNING":
                pid = previous_pid
                path = Path(str(current.get("lock_path") or self._node_lock_path()))
                try:
                    marker = path.read_text(encoding="ascii")
                except (FileNotFoundError, OSError) as exc:
                    raise OperatorControlError("NODE_STOP_UNSAFE") from exc
                try:
                    marker_pid = int(marker.splitlines()[0].strip())
                except (IndexError, ValueError) as exc:
                    raise OperatorControlError("NODE_STOP_UNSAFE") from exc
                if not marker or marker_pid != pid:
                    raise OperatorControlError("NODE_STOP_UNSAFE")
                stop_path = Path(str(self.db_path) + ".stop")
                try:
                    stop_path.write_text(marker, encoding="ascii")
                except OSError as exc:
                    raise OperatorControlError("NODE_STOP_REQUEST_FAILED") from exc
                deadline = time.monotonic() + 15.0
                while time.monotonic() < deadline:
                    observed = self._node_status()
                    if observed["status"] in {"STOPPED", "STALE"}:
                        break
                    time.sleep(0.1)
                if self._node_status()["status"] not in {"STOPPED", "STALE"}:
                    raise OperatorControlError("NODE_STOP_TIMEOUT")
            replacement = self.ensure_node()
            replacement = dict(replacement)
            replacement["previous_pid"] = previous_pid or None
            replacement["previous_revision"] = previous_revision
            replacement_identity = replacement.get("process_identity")
            previous_identity = current.get("process_identity")
            run_identity_changed = bool(
                replacement_identity
                and (
                    not previous_identity
                    or str(replacement_identity) != str(previous_identity)
                )
            )
            started_at_changed = bool(
                replacement.get("started_at")
                and (
                    not current.get("started_at")
                    or str(replacement["started_at"]) != str(current["started_at"])
                )
            )
            replacement["restart_revision"] = replacement.get("revision")
            replacement["restart_confirmed"] = bool(
                replacement.get("pid")
                and (not previous_pid or int(replacement["pid"]) != previous_pid)
                and (run_identity_changed or started_at_changed)
            )
            if not replacement["restart_confirmed"]:
                raise OperatorControlError("NODE_RESTART_NOT_CONFIRMED")
            return replacement

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
    def _shadow_jobs_status(self) -> dict[str, Any]:
        """Return a bounded, read-only status view of persisted shadow jobs."""
        loader = getattr(self.store, "list_shadow_jobs", None)
        if not callable(loader):
            rows: list[Any] = []
        else:
            try:
                rows = loader(limit=128)
            except TypeError:
                try:
                    rows = loader(None, 128)
                except (AttributeError, TypeError, ValueError, sqlite3.Error):
                    rows = []
            except (AttributeError, TypeError, ValueError, sqlite3.Error):
                rows = []
        statuses = ("REGISTERED", "RUNNING", "WAITING_FOR_DATA", "COMPLETED", "BLOCKED", "STOPPED")
        counts = {name: 0 for name in statuses}
        jobs: list[dict[str, Any]] = []
        for row in rows if isinstance(rows, (list, tuple)) else []:
            if not isinstance(row, Mapping):
                continue
            status = str(row.get("status") or "UNKNOWN").strip().upper()
            if status not in counts:
                status = "UNKNOWN"
            if status in counts:
                counts[status] += 1
            state = row.get("state")
            raw_blockers = state.get("blockers")
            blockers = []
            for value in (raw_blockers if isinstance(raw_blockers, (list, tuple)) else []):
                text = str(value).strip()[:256]
                if text:
                    blockers.append(text.split(":", 1)[0].strip()[:128])
            blockers = blockers[:16]
            last_blocker_text = str(state.get("last_blocker") or "").strip()[:256]
            last_blocker = (
                last_blocker_text.split(":", 1)[0].strip()[:128]
                if last_blocker_text
                else (blockers[-1] if blockers else None)
            )
            stop = state.get("stop")
            stop = stop if isinstance(stop, Mapping) else {}
            def nonnegative_int(value: Any) -> int | None:
                if isinstance(value, bool):
                    return None
                try:
                    parsed = int(value)
                except (TypeError, ValueError, OverflowError):
                    return None
                return parsed if parsed >= 0 else None
            cycles = nonnegative_int(state.get("cycles")) or 0
            observations = nonnegative_int(state.get("public_observations")) or 0
            cycle_limit = nonnegative_int(stop.get("max_cycles"))
            observation_limit = nonnegative_int(stop.get("max_observations"))
            jobs.append(
                {
                    "job_id": str(row.get("job_id") or "").strip()[:256] or None,
                    "status": status,
                    "cycles": cycles,
                    "public_observations": observations,
                    "max_cycles": cycle_limit,
                    "max_observations": observation_limit,
                    "stop_at": stop.get("stop_at"),
                    "stop_reached": stop.get("reached") is True,
                    "stop_reason": str(stop.get("reason") or "").strip()[:256] or None,
                    "blocker": last_blocker,
                    "blockers": blockers,
                    "next_evaluation_at": row.get("next_evaluation_at") or state.get("next_evaluation_at"),
                    "updated_at": row.get("updated_at"),
                    "next_action": (
                        "WAIT_FOR_SHADOW_WORKER"
                        if status == "REGISTERED"
                        else "WAIT_FOR_NEXT_EVALUATION"
                        if status == "RUNNING"
                        else "WAIT_FOR_DATA"
                        if status == "WAITING_FOR_DATA"
                        else "REVIEW_BLOCKER"
                        if status == "BLOCKED"
                        else "NO_ACTION_COMPLETED"
                        if status == "COMPLETED"
                        else "NO_ACTION_STOPPED"
                        if status == "STOPPED"
                        else "REVIEW_SHADOW_JOB"
                    ),
                }
            )
        active_statuses = {"REGISTERED", "RUNNING", "WAITING_FOR_DATA"}
        current = next((item for item in jobs if item["status"] in active_statuses), None)
        return {
            "total": len(jobs),
            "active": sum(1 for item in jobs if item["status"] in active_statuses),
            "status_counts": counts,
            "current_job_id": current.get("job_id") if current else None,
            "current_status": current.get("status") if current else None,
            "current_job": dict(current) if current else None,
            "next_evaluation_at": current.get("next_evaluation_at") if current else None,
            "next_action": current.get("next_action") if current else None,
            "jobs": jobs,
            "read_only": True,
            "paper_only": True,
            "live_execution": False,
        }

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
            raw_status = str(row.get("status") or "NOT_STARTED").upper()
            heartbeat = row.get("heartbeat_at")
            age: float | None = None
            try:
                stamp = datetime.fromisoformat(str(heartbeat).replace("Z", "+00:00"))
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                age = max(
                    0.0,
                    (datetime.now(timezone.utc) - stamp.astimezone(timezone.utc)).total_seconds(),
                )
            except (TypeError, ValueError, OverflowError):
                pass
            try:
                stale_after = float(payload.get("stale_after_seconds", 300.0))
            except (TypeError, ValueError):
                stale_after = 300.0
            status = (
                "STALE"
                if raw_status == "RUNNING" and (age is None or age > stale_after)
                else raw_status
            )
            return {
                "status": status,
                "pid": payload.get("pid"),
                "heartbeat_at": heartbeat,
                "heartbeat_age_seconds": age,
                "started_at": row.get("started_at"),
                "last_error_code": payload.get("last_error_code"),
                "last_successful_tick": payload.get("last_successful_tick"),
                "next_retry_at": payload.get("next_retry_at"),
                "consecutive_failures": payload.get("consecutive_failures"),
                "revision": payload.get("revision"),
                "process_identity": payload.get("process_identity"),
            }


        report: Mapping[str, Any] = {}
        canary = self._canary_service
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
        readiness_report = _operator_patch_non_missing(
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
        # Authoritative report sections override stale cached aliases while
        # preserving legacy-only readiness metrics.
        for section in (control_report, worker_report, execution_report):
            canary_status = _operator_patch_non_missing(canary_status, section)
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
            or {},
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
        shadow_worker = worker("shadow-assessment")
        shadow_worker["worker_name"] = "shadow-assessment"
        shadow_worker["paper_only"] = True
        shadow_worker["live_execution"] = False
        raw_shadow_worker = worker_map.get("shadow-assessment", {})
        shadow_payload = (
            raw_shadow_worker.get("payload")
            if isinstance(raw_shadow_worker, Mapping)
            and isinstance(raw_shadow_worker.get("payload"), Mapping)
            else {}
        )
        for field in (
            "configured_interval_seconds",
            "jobs_per_cycle",
            "active_job_count",
            "due_job_count",
            "selected_job_count",
            "processed_jobs",
            "successful_jobs",
            "blocked_jobs",
            "waiting_for_data_jobs",
            "completed_jobs",
            "failed_jobs",
            "next_evaluation_at",
            "next_work",
        ):
            if field not in shadow_payload:
                continue
            value = shadow_payload.get(field)
            if field == "next_work":
                shadow_worker[field] = str(value).strip()[:256] if value is not None else None
            elif field.endswith("_at"):
                shadow_worker[field] = value
            else:
                try:
                    number = float(value)
                    if not math.isfinite(number) or number < 0:
                        continue
                    shadow_worker[field] = int(number) if number.is_integer() else number
                except (TypeError, ValueError, OverflowError):
                    continue
        raw_worker_blockers = shadow_payload.get("blockers")
        if isinstance(raw_worker_blockers, (list, tuple)):
            projected_blockers = []
            for value in raw_worker_blockers[:16]:
                text = str(value).strip()[:256]
                if text:
                    projected_blockers.append(text.split(":", 1)[0].strip()[:128])
            shadow_worker["blockers"] = projected_blockers
        raw_job_statuses = shadow_payload.get("job_statuses")
        if isinstance(raw_job_statuses, (list, tuple)):
            projected_jobs = []
            for item in raw_job_statuses[:64]:
                if not isinstance(item, Mapping):
                    continue
                blocker_text = str(item.get("blocker") or "").strip()[:256]
                projected_jobs.append(
                    {
                        "job_id": str(item.get("job_id") or "").strip()[:256] or None,
                        "status": str(item.get("status") or "UNKNOWN").strip().upper(),
                        "blocker": blocker_text.split(":", 1)[0].strip()[:128] if blocker_text else None,
                        "next_evaluation_at": item.get("next_evaluation_at"),
                    }
                )
            shadow_worker["job_statuses"] = projected_jobs
        shadow_jobs = self._shadow_jobs_status()
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
        try:
            settings_snapshot = self.risk_settings_snapshot()
        except Exception as exc:
            settings_snapshot = {
                "status": "ERROR",
                "error": type(exc).__name__,
                "detail": str(exc)[:160],
            }
        effective_settings = (
            settings_snapshot.get("effective_limits")
            if isinstance(settings_snapshot, Mapping)
            else None
        )
        if isinstance(effective_settings, Mapping):
            current_limits = dict(effective_settings)
            canary_status["risk_envelope"] = current_limits
            canary_status["risk_limits"] = dict(current_limits)
        try:
            rolling_state = self.rolling_portfolio_state()
        except Exception as exc:
            rolling_state = {
                "status": "ERROR",
                "blocker": type(exc).__name__.upper(),
            }
        rolling_worker = worker("rolling-portfolio")
        rolling_worker["scheduled"] = True
        rolling_worker["paper_only"] = True
        rolling_worker["live_execution"] = False
        canary_submit = (
            "AUTONOMOUS_WORKER"
            if autonomous_state.get("enabled")
            else "DISABLED_UNTIL_OPERATOR_ENABLE"
        )
        if self.execution_profile != PRODUCTION_EXECUTION_PROFILE:
            canary_submit = "DISABLED_ISOLATED_EXECUTION_PROFILE"
        authorization = self.execution_authorization_snapshot()
        lease = self.controller_lease_status()
        node_status = self._node_status(verify_identity=False)
        control_state = str(
            canary_status.get("control_state")
            or canary_status.get("micro_live_canary")
            or "DISARMED"
        ).upper()
        auth_active = authorization.get("active")
        auth_status = (
            str(auth_active.get("status") or "").upper()
            if isinstance(auth_active, Mapping)
            else ""
        )
        rolling_status = str(
            rolling_state.get("selection_status")
            or rolling_state.get("status")
            or ""
        ).upper()
        current_selection = rolling_state.get("selection")
        current_selection = (
            current_selection if isinstance(current_selection, Mapping) else {}
        )
        current_members = current_selection.get(
            "members",
            current_selection.get("selected_members", ()),
        )
        current_member_count = (
            len(current_members)
            if isinstance(current_members, (list, tuple))
            else 0
        )
        selection_state = str(
            current_selection.get("selection_status")
            or current_selection.get("status")
            or rolling_state.get("selection_status")
            or ""
        ).strip().upper()
        current_selection_available = bool(
            current_member_count
            and selection_state not in {"NONE", "STALE", "UNKNOWN", "OBSERVE"}
            and current_selection.get("selection_valid") is not False
        )
        if not current_selection_available:
            operator_mode = "observing"
        elif (
            control_state in {"AUTONOMOUS_MICRO_LIVE", "LIVE", "ARMED"}
            and auth_status == "ACTIVE"
        ):
            operator_mode = "live_authorized"
        elif auth_status == "ACTIVE":
            operator_mode = "exploratory_reviewed"
        elif rolling_status in {"CURRENT", "READY"} or rolling_state.get("actionable", 0):
            operator_mode = "evidence_selected"
        else:
            operator_mode = "observing"
        service_identity = _operator_service_identity(CanaryService)
        identity = {
            "instance_id": str(
                node_status.get("process_identity")
                or f"axiom-node:{node_status.get('pid') or 'unknown'}"
            ),
            "database": self.db_path,
            "db_path": self.db_path,
            "service": service_identity,
            "service_identity": service_identity,
            "revision": node_status.get("revision") or os.environ.get("AXIOM_REVISION") or "unknown",
            "process_identity": node_status.get("process_identity"),
        }
        limits = dict(
            settings_snapshot.get("effective_limits", {})
            if isinstance(settings_snapshot, Mapping)
            else {}
        )
        usage = dict(
            settings_snapshot.get("usage", {})
            if isinstance(settings_snapshot, Mapping)
            and isinstance(settings_snapshot.get("usage"), Mapping)
            else {}
        )
        remaining = dict(
            settings_snapshot.get("remaining", {})
            if isinstance(settings_snapshot, Mapping)
            and isinstance(settings_snapshot.get("remaining"), Mapping)
            else {}
        )
        skip_reasons = autonomous_state.get("signal_scan_reason_counts")
        if not isinstance(skip_reasons, Mapping):
            skip_reasons = {}
        economic_policy = {
            "mode": operator_mode,
            "policy": rolling_state.get("policy_identity")
            or rolling_state.get("active_policy_identity")
            or {},
            "limits": limits,
            "usage": usage,
            "remaining": remaining,
            "daily": {
                "submissions": {
                    "limit": limits.get("max_submitted_orders_per_day"),
                    "used": usage.get("submitted_orders"),
                    "remaining": remaining.get("submitted_orders"),
                },
                "buy_usd": {
                    "limit": limits.get("max_gross_daily_buy_usd"),
                    "used": usage.get("gross_daily_buy_usd"),
                    "remaining": remaining.get("gross_daily_buy_usd"),
                },
            },
            "lifetime": {
                "buy_usd": {
                    "limit": settings_snapshot.get("cumulative_buy_cap_usd")
                    if isinstance(settings_snapshot, Mapping)
                    else None,
                    "used": usage.get("cumulative_buy_usd"),
                    "remaining": settings_snapshot.get("remaining_cumulative_buy_usd")
                    if isinstance(settings_snapshot, Mapping)
                    else None,
                }
            },
        }
        next_work = (
            rolling_worker.get("next_work")
            or autonomous_state.get("next_work")
            or rolling_worker.get("next_retry_at")
        )
        last_evaluation = (
            rolling_worker.get("last_tick_completed_at")
            or rolling_worker.get("last_successful_tick")
            or autonomous_state.get("last_tick_completed_at")
        )
        if node_status.get("status") != "RUNNING":
            next_action = "LAUNCH_OBSERVATION"
        elif isinstance(authorization.get("draft"), Mapping) and str(
            authorization["draft"].get("status") or ""
        ).upper() == "DRAFT":
            next_action = "ACTIVATE_REVIEWED_AUTHORIZATION"
        elif operator_mode == "evidence_selected":
            next_action = "REVIEW_OR_ENABLE_EVIDENCE_SELECTED_POLICY"
        else:
            next_action = "CONTINUE_OBSERVATION"
        raw_blocker = canary_status.get("blocker")
        if isinstance(raw_blocker, Mapping):
            blocker_code = str(
                raw_blocker.get("code")
                or raw_blocker.get("reason")
                or raw_blocker.get("category")
                or "UNKNOWN"
            ).strip().upper()
            blocker_resolver = str(
                raw_blocker.get("resolver")
                or raw_blocker.get("action")
                or "REVIEW_OPERATOR_STATUS"
            ).strip()
        else:
            blocker_code = str(raw_blocker or "").strip().upper() or "NONE"
            blocker_resolver = (
                "LAUNCH_OBSERVATION"
                if blocker_code == "AUTONOMOUS_CANARY_DISABLED"
                else "REVIEW_PERSISTED_EVIDENCE"
                if blocker_code in {"NO_SIGNAL", "NO_DATA", "DATA_UNAVAILABLE"}
                else "REVIEW_OPERATOR_STATUS"
            )
        if blocker_code in {"NONE", "OK", "READY"}:
            blocker_category = "NONE"
        elif "AUTH" in blocker_code or "REVIEW" in blocker_code:
            blocker_category = "AUTHORIZATION"
        elif any(token in blocker_code for token in ("CONFIG", "SETTING", "PROFILE")):
            blocker_category = "CONFIG"
        elif any(token in blocker_code for token in ("CAPITAL", "BALANCE", "BUDGET", "LIMIT", "RISK")):
            blocker_category = "CAPITAL"
        elif any(token in blocker_code for token in ("DATA", "MARKET", "DEPTH", "SIGNAL")):
            blocker_category = "DATA"
        elif any(token in blocker_code for token in ("ERROR", "EXCEPTION", "DEFECT", "INVARIANT")):
            blocker_category = "DEFECT"
        else:
            blocker_category = "EXECUTION"
        blocker_detail = {
            "code": blocker_code,
            "category": blocker_category,
            "resolver": blocker_resolver,
            "action": next_action,
            "next_scheduled_action": str(
                next_work or rolling_worker.get("next_review_at") or next_action
            ),
        }
        coverage = dict(market_scope_funnel)
        coverage.setdefault("exclusions", market_scope_funnel.get("exclusions", []))
        coverage_summary_loader = getattr(self.store, "dashboard_coverage_summary", None)
        if callable(coverage_summary_loader):
            try:
                coverage_summary = coverage_summary_loader()
            except Exception:
                coverage_summary = {}
            if isinstance(coverage_summary, Mapping):
                # Qualification funnel fields and dataset-count compatibility
                # fields are distinct durable projections; keep both.
                for key in (
                    "historical_count",
                    "historical_datasets",
                    "historical_rows",
                    "forward_count",
                    "forward_datasets",
                    "forward_rows",
                ):
                    if key not in coverage and coverage_summary.get(key) is not None:
                        coverage[key] = coverage_summary[key]
        canary_status["blocker_detail"] = blocker_detail
        autonomous_state["blocker_detail"] = dict(blocker_detail)
        selection = rolling_state.get("selection")
        selection = selection if isinstance(selection, Mapping) else {}
        active_strategies = (
            rolling_state.get("active_rows")
            if isinstance(rolling_state.get("active_rows"), (list, tuple))
            else selection.get("members", selection.get("selected_members", []))
        )
        active_strategies = (
            list(active_strategies)
            if isinstance(active_strategies, (list, tuple))
            else []
        )
        suspended_strategies = rolling_state.get(
            "suspended_strategies",
            rolling_state.get("suspended_rows", []),
        )
        if not isinstance(suspended_strategies, (list, tuple)):
            suspended_strategies = [
                item
                for item in active_strategies
                if isinstance(item, Mapping)
                and str(item.get("status") or "").strip().upper()
                in {"SUSPENDED", "PAUSED", "BLOCKED"}
            ]
        suspended_strategies = list(suspended_strategies)
        execution_summary = {
            "orders": execution_report.get("orders", execution_report.get("trades", [])),
            "partial_fills": execution_report.get("partial_fills", []),
            "positions": execution_report.get("positions", []),
            "exits": execution_report.get("exits", []),
            "net_pnl": execution_report.get(
                "net_pnl", execution_report.get("net_pnl_usd")
            ),
        }
        strategies_projection = {
            "active": active_strategies,
            "suspended": suspended_strategies,
            "selection": rolling_state.get("selection"),
        }
        signals_projection = {
            "latest": latest_signal,
            "current": latest_signal,
            "skip_reasons": dict(skip_reasons),
            "category_skip_reasons": dict(skip_reasons),
        }
        execution_state = {
            "state": control_state,
            "mode": operator_mode,
            "submit": canary_submit,
            "summary": execution_summary,
            "live_execution": False,
        }
        last_review = (
            rolling_state.get("last_review_at")
            or rolling_state.get("reviewed_at")
            or rolling_state.get("active_at")
        )
        try:
            exploratory_live_review = self.exploratory_live_review_snapshot()
        except Exception as exc:
            exploratory_live_review = {
                "profitability": "UNPROVEN",
                "status": "BLOCKED",
                "blockers": [type(exc).__name__.upper()],
                "paper_only": True,
                "live_execution": False,
            }
        exploratory_live_enabled = (
            str(control_state or "").upper() in {"AUTONOMOUS_MICRO_LIVE", "LIVE"}
            or exploratory_live_review.get("live_execution") is True
        )
        execution_state["live_execution"] = exploratory_live_enabled
        execution_state["paper_only"] = not exploratory_live_enabled
        return {
            "execution_profile": self.execution_profile,
            "identity": identity,
            "revision": identity.get("revision"),
            "blocker": blocker_detail,
            "blockers": [blocker_detail] if blocker_code != "NONE" else [],
            "instance": identity,
            "mode": operator_mode,
            "armed": control_state in {"ARMED", "AUTONOMOUS_MICRO_LIVE", "LIVE"},
            "armed_state": control_state,
            "economic_policy": economic_policy,
            "policy": economic_policy.get("policy", {}),
            "budgets": economic_policy,
            "live_execution": execution_state["live_execution"],
            "paper_only": execution_state["paper_only"],
            "exploratory_live_review": exploratory_live_review,
            "limits": limits,
            "daily_budget": economic_policy.get("daily", {}),
            "lifetime_budget": economic_policy.get("lifetime", {}),
            "usage": usage,
            "remaining": remaining,
            "remaining_budgets": remaining,
            "authorization": authorization,
            "execution_authorization": authorization,
            "execution_authorization_id": authorization.get("authorization_id")
            if isinstance(authorization, Mapping)
            else None,
            "controller_lease": lease,
            "coverage": coverage,
            "qualification_coverage": coverage,
            "historical_count": coverage.get("historical_count", 0),
            "historical_rows": coverage.get("historical_rows", 0),
            "forward_count": coverage.get("forward_count", 0),
            "forward_rows": coverage.get("forward_rows", 0),
            "strategies": strategies_projection,
            "active_strategies": strategies_projection["active"],
            "suspended_strategies": strategies_projection["suspended"],
            "signals": signals_projection,
            "current_signals": signals_projection["current"],
            "execution_summary": execution_summary,
            "execution": execution_summary,
            "execution_state": execution_state,
            "last_review": last_review,
            "last_review_at": last_review,
            "last_evaluation": last_evaluation,
            "last_work": last_evaluation,
            "work": {"last": last_evaluation, "next": next_work},
            "next_work": next_work,
            "next_review": rolling_worker.get("next_review_at")
            or rolling_state.get("review_due_at"),
            "next_review_at": rolling_worker.get("next_review_at")
            or rolling_state.get("review_due_at"),
            "actions": self._action_snapshot(),
            "risk_settings": settings_snapshot,
            "canary_status_report": dict(report),
            "connectivity": latest_connectivity,
            "hermes": hermes.state(),
            "collector": worker("polymarket-collector"),
            "paper": {**worker("paper-engine"), "read_only": True, "live_execution": False},
            "node": node_status,
            "autonomous_canary_worker": worker_status,
            "shadow_worker": shadow_worker,
            "shadow_jobs": shadow_jobs,
            "shadow": {
                "worker": dict(shadow_worker),
                "jobs": dict(shadow_jobs),
                "current_job_id": shadow_jobs.get("current_job_id"),
                "next_action": shadow_jobs.get("next_action"),
                "read_only": True,
                "paper_only": True,
                "live_execution": False,
            },
            "rolling_portfolio": dict(rolling_state),
            "rolling_portfolio_worker": rolling_worker,
            "market_scope_funnel": market_scope_funnel,
            "credentials": credentials,
            "canary": {
                "blocker_detail": blocker_detail,
                "status": canary_status,
                "status_report": dict(report),
                "control": dict(control_report),
                "readiness": dict(readiness_report),
                "worker": dict(worker_report),
                "execution": dict(execution_report),
                "settings": settings_snapshot,
                "latest_signal": latest_signal,
                "autonomous": autonomous_state,
                "connectivity": latest_connectivity,
                "submit": canary_submit,
            },
            "live_execution": execution_state["live_execution"],
            "paper_only": execution_state["paper_only"],
        }

    def _audit(
        self,
        action: str,
        target: str,
        *,
        success: bool,
        reason: str = "",
        result: Mapping[str, Any] | None = None,
    ) -> str | None:
        try:
            return self.store.record_operator_action(
                action,
                target,
                success=success,
                reason=reason,
                result=_safe_value(result or {}),
            )
        except Exception:
            return None
    def _enforce_action_fence(self, action: str) -> None:
        """Reject production-boundary actions outside exact production."""
        if (
            action in _ISOLATED_OPERATOR_BLOCKED_ACTIONS
            and self.execution_profile != PRODUCTION_EXECUTION_PROFILE
        ):
            raise OperatorControlError("ISOLATED_EXECUTION_PROFILE")

    def execute(
        self,
        action: str,
        target: str = "",
        *,
        confirm: str = "",
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        requested_action_value = str(action or "").strip()
        action_value = _ACTION_ALIASES.get(
            requested_action_value,
            requested_action_value,
        )
        target_value = str(target or "").strip()
        if payload is not None and not isinstance(payload, Mapping):
            return {
                "ok": False,
                "action": requested_action_value,
                "target": target_value,
                "reason": "INVALID_ACTION_PAYLOAD",
                "paper_only": True,
                "live_execution": False,
            }
        action_payload = dict(payload or {})
        action_id: str | None = None
        try:
            self._enforce_action_fence(action_value)
            if action_value not in _ALLOWED_ACTIONS:
                raise OperatorControlError("ACTION_NOT_ALLOWED")
            if action_value in {"canary.settings.save_draft", "risk.settings.save_draft"}:
                allowed = {"values", "actor"}
                if set(action_payload) - allowed:
                    raise OperatorControlError("UNSUPPORTED_RISK_SETTINGS_FIELDS")
                values = action_payload.get("values")
                if not isinstance(values, Mapping):
                    raise OperatorControlError("RISK_SETTINGS_VALUES_REQUIRED")
            elif action_value in {"canary.settings.activate_draft", "risk.settings.activate_draft"}:
                allowed = {"config_id", "actor", "expected_generation"}
                if set(action_payload) - allowed:
                    raise OperatorControlError("UNSUPPORTED_RISK_SETTINGS_FIELDS")
                if not isinstance(action_payload.get("config_id"), str):
                    raise OperatorControlError("RISK_SETTINGS_CONFIG_REQUIRED")
                config_id_value = _safe_identifier(
                    action_payload["config_id"],
                    "risk settings config ID",
                )
                generation_value = _positive_generation(
                    action_payload.get("expected_generation"),
                    "RISK_SETTINGS_GENERATION_REQUIRED",
                )
                # The config identity is the action target.  This keeps
                # concurrent activation attempts for different drafts
                # independent while exact retries still deduplicate.
                target_value = f"{config_id_value}:{generation_value}"
            elif action_value == "rolling.admission.review":
                allowed = {
                    "policy",
                    "values",
                    "actor",
                    "expected_risk_config_id",
                    "expected_risk_config_generation",
                    "expected_risk_config_hash",
                }
                if set(action_payload) - allowed:
                    raise OperatorControlError("UNSUPPORTED_ROLLING_POLICY_FIELDS")
                policy_values = action_payload.get("policy", action_payload.get("values"))
                if policy_values is not None and not isinstance(policy_values, Mapping):
                    raise OperatorControlError("ROLLING_POLICY_REQUIRED")
                target_value = _rolling_review_target(action_payload)
                if action_payload.get("expected_risk_config_generation") is not None:
                    _positive_generation(
                        action_payload["expected_risk_config_generation"],
                        "ROLLING_RISK_GENERATION_REQUIRED",
                    )
            elif action_value == "rolling.admission.activate":
                allowed = {
                    "policy_id",
                    "policy_version",
                    "draft_id",
                    "draft_version",
                    "actor",
                    "expected_risk_config_id",
                    "expected_risk_config_generation",
                    "expected_risk_config_hash",
                }
                if set(action_payload) - allowed:
                    raise OperatorControlError("UNSUPPORTED_ROLLING_POLICY_FIELDS")
                policy_fields_present = (
                    "policy_id" in action_payload or "policy_version" in action_payload
                )
                draft_fields_present = (
                    "draft_id" in action_payload or "draft_version" in action_payload
                )
                has_policy_identity = bool(
                    isinstance(action_payload.get("policy_id"), str)
                    and action_payload["policy_id"].strip()
                ) and bool(
                    isinstance(action_payload.get("policy_version"), str)
                    and action_payload["policy_version"].strip()
                )
                has_draft_identity = bool(
                    isinstance(action_payload.get("draft_id"), str)
                    and action_payload["draft_id"].strip()
                ) and bool(
                    isinstance(action_payload.get("draft_version"), str)
                    and action_payload["draft_version"].strip()
                )
                if (
                    (policy_fields_present and not has_policy_identity)
                    or (draft_fields_present and not has_draft_identity)
                    or (not has_policy_identity and not has_draft_identity)
                ):
                    raise OperatorControlError("ROLLING_POLICY_IDENTITY_REQUIRED")
                if has_policy_identity:
                    _safe_identifier(action_payload["policy_id"], "policy ID")
                    _safe_identifier(action_payload["policy_version"], "policy version")
                if has_draft_identity:
                    _safe_identifier(action_payload["draft_id"], "rolling draft ID")
                    _safe_identifier(action_payload["draft_version"], "rolling draft version")
                if action_payload.get("expected_risk_config_generation") is not None:
                    _positive_generation(
                        action_payload["expected_risk_config_generation"],
                        "ROLLING_RISK_GENERATION_REQUIRED",
                    )
            elif action_value == "execution_authorization.review":
                allowed = {"values", "actor"}
                if set(action_payload) - allowed:
                    raise OperatorControlError("UNSUPPORTED_EXECUTION_AUTHORIZATION_FIELDS")
                values = action_payload.get("values")
                if values is not None and not isinstance(values, Mapping):
                    raise OperatorControlError("EXECUTION_AUTHORIZATION_VALUES_REQUIRED")
                target_value = "exploratory:" + self._rolling_canonical_hash(
                    values if isinstance(values, Mapping) else {}
                )[:32]
            elif action_value in {
                "execution_authorization.activate",
                "execution_authorization.revoke",
            }:
                allowed = {"authorization_id", "actor", "expected_generation", "reason"}
                if set(action_payload) - allowed:
                    raise OperatorControlError("UNSUPPORTED_EXECUTION_AUTHORIZATION_FIELDS")
                is_revoke = action_value.endswith("revoke")
                if is_revoke:
                    current = self.execution_authorization_snapshot().get("active")
                    required_reason = "EXECUTION_AUTHORIZATION_ACTIVE_REQUIRED"
                else:
                    get_config = getattr(self.store, "get_operator_config", None)
                    current = (
                        get_config("execution_authorization_review", None)
                        if callable(get_config)
                        else None
                    )
                    required_reason = "EXECUTION_AUTHORIZATION_DRAFT_REQUIRED"
                current = current if isinstance(current, Mapping) else {}
                current_status = str(current.get("status") or "").upper()
                expected_status = "ACTIVE" if is_revoke else "DRAFT"
                if current_status != expected_status:
                    raise OperatorControlError(required_reason)
                current_id = str(
                    current.get("authorization_id") or current.get("id") or ""
                ).strip()
                if not current_id:
                    raise OperatorControlError(required_reason)
                supplied_id = action_payload.get("authorization_id")
                if supplied_id is not None and str(supplied_id).strip() != current_id:
                    raise OperatorControlError("EXECUTION_AUTHORIZATION_BINDING_STALE")
                target_value = f"exploratory:{current_id}"
                if action_payload.get("expected_generation") is not None:
                    generation = _positive_generation(
                        action_payload["expected_generation"],
                        "EXECUTION_AUTHORIZATION_GENERATION_REQUIRED",
                    )
                    if current.get("generation") is not None and int(current["generation"]) != generation:
                        raise OperatorControlError(
                            "EXECUTION_AUTHORIZATION_GENERATION_CHANGED"
                        )
            elif action_value == "exploratory.live.review_confirm":
                allowed = {"actor", "market_id", "token_id", "candidate_id"}
                if set(action_payload) - allowed:
                    raise OperatorControlError("UNSUPPORTED_EXPLORATORY_LIVE_FIELDS")
                target_value = "exploratory-live"
            if action_value == RECOVERY_ACTION:
                allowed = {"event_id", "signal_id", "exchange_order_id"}
                if set(action_payload) - allowed:
                    raise OperatorControlError("UNSUPPORTED_RECOVERY_FIELDS")
                if self.execution_profile != PRODUCTION_EXECUTION_PROFILE:
                    raise OperatorControlError("RECOVERY_PRODUCTION_PROFILE_REQUIRED")
                try:
                    event_value = recovery_identifier(
                        action_payload.get("event_id"), "event ID"
                    )
                    signal_value = recovery_identifier(
                        action_payload.get("signal_id"), "signal ID"
                    )
                    order_value = recovery_identifier(
                        action_payload.get("exchange_order_id"), "exchange order ID"
                    )
                    normalize_recovery_profile(
                        {"environment": PRODUCTION_EXECUTION_PROFILE, "allow_environment": False}
                    )
                except RecoveryProfileError as exc:
                    raise OperatorControlError(exc.code) from exc
                target_value = event_value
            if action_value not in _ALLOWED_ACTIONS:
                raise OperatorControlError("ACTION_NOT_ALLOWED")
            expected = _CONFIRMATIONS.get(action_value)
            if (
                expected is not None
                and not (action_value == "canary.enable_auto" and action_payload)
                and confirm != expected
            ):
                raise OperatorControlError("EXACT_CONFIRMATION_REQUIRED")
            if action_value == "rolling.admission.activate":
                if has_draft_identity:
                    target_value = (
                        f"{action_payload['draft_id']}:{action_payload['draft_version']}"
                    )
                else:
                    target_value = (
                        f"{action_payload['policy_id']}:{action_payload['policy_version']}"
                    )
            if action_value in {"canary.eligibility.verify", "canary.eligibility.mark", "canary.generate_signal", "canary.arm"}:
                target_value = _safe_identifier(target_value, "candidate ID")
            elif action_value == "canary.enable_auto":
                if target_value and not action_payload:
                    raise OperatorControlError("AUTONOMOUS_CANARY_ACCEPTS_NO_TARGET")
                if action_payload:
                    allowed = {"venue", "config_id", "expected_generation"}
                    if set(action_payload) - allowed:
                        raise OperatorControlError("UNSUPPORTED_CANARY_ENABLE_FIELDS")
                    if not isinstance(action_payload.get("venue"), str):
                        raise OperatorControlError("UNSUPPORTED_CANARY_VENUE")
                    venue_name = action_payload["venue"].strip().lower()
                    if not isinstance(action_payload.get("config_id"), str):
                        raise OperatorControlError("RISK_SETTINGS_CONFIG_REQUIRED")
                    config_id = _safe_identifier(
                        action_payload["config_id"],
                        "canary config ID",
                    )
                    expected_generation = _positive_generation(
                        action_payload.get("expected_generation"),
                        "CANARY_GENERATION_REQUIRED",
                    )
                    if venue_name != "polymarket":
                        raise OperatorControlError("UNSUPPORTED_CANARY_VENUE")
                    exact_confirmation = (
                        f"ENABLE AUTO CANARY {venue_name.upper()} {config_id} {expected_generation}"
                    )
                    if confirm != exact_confirmation:
                        raise OperatorControlError("EXACT_CONFIRMATION_REQUIRED")
                    target_value = f"{venue_name}:{config_id}:{expected_generation}"
            if action_value in _ALLOWED_ACTIONS:
                action_id, duplicate = self._begin_action(action_value, target_value)
                if duplicate is not None:
                    return {
                        "ok": False,
                        "action": requested_action_value,
                        "target": target_value,
                        "action_id": action_id,
                        "action_status": "RUNNING",
                        "reason": "ACTION_ALREADY_RUNNING",
                        "paper_only": True,
                        "live_execution": False,
                    }
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
            elif action_value in {"canary.settings.save_draft", "risk.settings.save_draft"}:
                result = {
                    "risk_settings": self.save_risk_settings_draft(
                        action_payload["values"],
                        actor=action_payload.get("actor", "operator"),
                    )
                }
            elif action_value in {"canary.settings.activate_draft", "risk.settings.activate_draft"}:
                result = {
                    "risk_settings": self.activate_risk_settings_draft(
                        action_payload["config_id"],
                        actor=action_payload.get("actor", "operator"),
                        expected_generation=action_payload.get("expected_generation"),
                    )
                }
            elif action_value == "execution_authorization.review":
                result = {
                    "execution_authorization": self.review_execution_authorization(
                        action_payload.get("values"),
                        actor=action_payload.get("actor", "operator"),
                    )
                }
            elif action_value == "execution_authorization.activate":
                result = {
                    "execution_authorization": self.activate_execution_authorization(
                        action_payload.get("authorization_id"),
                        actor=action_payload.get("actor", "operator"),
                        expected_generation=action_payload.get("expected_generation"),
                    )
                }
            elif action_value == "execution_authorization.revoke":
                result = {
                    "execution_authorization": self.revoke_execution_authorization(
                        action_payload.get("authorization_id"),
                        actor=action_payload.get("actor", "operator"),
                        expected_generation=action_payload.get("expected_generation"),
                        reason=action_payload.get("reason", "operator_revoke"),
                    )
                }
            elif action_value == "exploratory.live.review_confirm":
                result = {
                    "exploratory_live": self.confirm_exploratory_live(
                        action_payload,
                        actor=action_payload.get("actor", "operator"),
                    )
                }
            elif action_value == "rolling.admission.review":
                result = {
                    "rolling_policy": self.review_rolling_admission_policy(
                        action_payload.get("policy", action_payload.get("values")),
                        actor=action_payload.get("actor", "operator"),
                        expected_risk_config_id=action_payload.get("expected_risk_config_id"),
                        expected_risk_config_generation=action_payload.get(
                            "expected_risk_config_generation"
                        ),
                        expected_risk_config_hash=action_payload.get("expected_risk_config_hash"),
                    )
                }
            elif action_value == "rolling.admission.activate":
                result = {
                    "rolling_policy": self.activate_rolling_admission_policy(
                        action_payload.get("policy_id"),
                        action_payload.get("policy_version"),
                        actor=action_payload.get("actor", "operator"),
                        draft_id=action_payload.get("draft_id"),
                        draft_version=action_payload.get("draft_version"),
                        expected_risk_config_id=action_payload.get("expected_risk_config_id"),
                        expected_risk_config_generation=action_payload.get(
                            "expected_risk_config_generation"
                        ),
                        expected_risk_config_hash=action_payload.get("expected_risk_config_hash"),
                    )
                }
            elif action_value == RECOVERY_ACTION:
                credentials = CredentialStore()
                if not credentials.configured(allow_environment=False):
                    raise OperatorControlError("CREDENTIALS_NOT_CONFIGURED")
                service = CanaryService(
                    self.store,
                    credentials=credentials,
                    initialize=True,
                    settings=self.settings,
                    profile={
                        "environment": PRODUCTION_EXECUTION_PROFILE,
                        "allow_environment": False,
                    },
                )
                venue = PolymarketClobV2Venue(allow_environment=False)
                result = {
                    "recovery": service.recover_entry_intent(
                        event_value,
                        order_value,
                        signal_id=signal_value,
                        venue=venue,
                        confirmation=confirm,
                    )
                }
            elif action_value == "canary.connectivity_check":
                with self._connectivity_lock:
                    credentials: CredentialStore | None = None
                    configured = False
                    current_fingerprint: str | None = None
                    try:
                        credentials = CredentialStore()
                        configured = bool(
                            credentials.configured(allow_environment=False)
                        )
                        if configured:
                            try:
                                current_values = credentials.load(
                                    allow_environment=False
                                )
                            except Exception:
                                configured = False
                            else:
                                if (
                                    not isinstance(current_values, Mapping)
                                    or not current_values.get("private_key")
                                    or not current_values.get("wallet_address")
                                ):
                                    configured = False
                                else:
                                    current_fingerprint = credential_fingerprint(
                                        current_values
                                    )
                        venue = (
                            PolymarketClobV2Venue(allow_environment=False)
                            if configured
                            else None
                        )
                        service = CanaryService(
                            self.store,
                            credentials=credentials,
                            initialize=False,
                        )
                        raw_connectivity = service.connectivity_check(
                            venue=venue,
                            allow_environment=False,
                        )
                        connectivity = _project_connectivity(
                            raw_connectivity,
                            checked_at=utc_now(),
                            authoritative_credentials_configured=configured,
                            authoritative_fingerprint=current_fingerprint,
                        )
                    except CanaryBlocked as exc:
                        connectivity = _blocked_connectivity_projection(
                            str(exc),
                            credentials_configured=configured,
                            authoritative_fingerprint=current_fingerprint,
                        )
                    except Exception:
                        connectivity = _blocked_connectivity_projection(
                            credentials_configured=configured,
                            authoritative_fingerprint=current_fingerprint,
                        )
                    self.store.set_operator_config(
                        CANARY_CONNECTIVITY_CONFIG_KEY,
                        connectivity,
                    )
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
                settings_snapshot = self.settings.snapshot()
                settings_config_id = settings_snapshot.get("config_id")
                settings_generation = settings_snapshot.get("generation")
                if not isinstance(settings_config_id, str):
                    raise OperatorControlError("RISK_SETTINGS_CONFIG_REQUIRED")
                settings_generation = _positive_generation(
                    settings_generation,
                    "RISK_SETTINGS_GENERATION_REQUIRED",
                )
                service = CanaryService(
                    self.store,
                    credentials=credentials,
                    initialize=True,
                    settings=self.settings,
                )
                venue = PolymarketClobV2Venue(allow_environment=False)
                result = {
                    "canary": service.arm(
                        target_value,
                        venue=venue,
                        config_id=settings_config_id,
                        expected_generation=settings_generation,
                    )
                }
            elif action_value == "canary.enable_auto":
                with self._connectivity_lock:
                    connectivity = _stored_connectivity_projection(
                        self.store.get_operator_config(CANARY_CONNECTIVITY_CONFIG_KEY, None),
                        require_fresh=True,
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
                    credentials = CredentialStore()
                    try:
                        values = credentials.load(allow_environment=False)
                    except Exception as exc:
                        raise OperatorControlError("CREDENTIALS_NOT_CONFIGURED") from exc
                    if (
                        not isinstance(values, Mapping)
                        or not values.get("private_key")
                        or not values.get("wallet_address")
                    ):
                        raise OperatorControlError("CREDENTIALS_NOT_CONFIGURED")
                    expected_fingerprint = connectivity["account"].get(
                        "credential_fingerprint"
                    )
                    if (
                        not isinstance(expected_fingerprint, str)
                        or credential_fingerprint(values) != expected_fingerprint
                    ):
                        raise OperatorControlError("CREDENTIAL_BINDING_MISMATCH")
                    if not action_payload:
                        raise OperatorControlError("CANARY_ENABLE_FIELDS")
                    service = CanaryService(
                        self.store,
                        credentials=credentials,
                        initialize=True,
                        settings=self.settings,
                    )
                    enable_kwargs: dict[str, Any] = {
                        "venue": action_payload["venue"].strip().lower(),
                        "config_id": _safe_identifier(
                            action_payload["config_id"],
                            "canary config ID",
                        ),
                        "expected_generation": _positive_generation(
                            action_payload["expected_generation"],
                            "CANARY_GENERATION_REQUIRED",
                        ),
                        "expected_credential_fingerprint": expected_fingerprint,
                    }
                    result = {
                        "canary": service.enable_autonomous_micro_live(**enable_kwargs),
                        "confirmation": confirm,
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
            self._finish_action(
                action_id,
                status="COMPLETE",
                result=public if isinstance(public, Mapping) else {},
            )
            audit_id = self._audit(action_value, target_value, success=True, result=public)
            response_live_execution = False
            response_paper_only = True
            if action_value == "exploratory.live.review_confirm":
                exploratory_result = result.get("exploratory_live") if isinstance(result, Mapping) else {}
                exploratory_result = (
                    exploratory_result if isinstance(exploratory_result, Mapping) else {}
                )
                response_live_execution = exploratory_result.get("live_execution") is True
                response_paper_only = not response_live_execution
            response = {
                "ok": True,
                "action": requested_action_value,
                "target": target_value,
                "action_id": action_id or audit_id,
                "action_status": "COMPLETE",
                "result": public,
                "paper_only": response_paper_only,
                "live_execution": response_live_execution,
            }
            if action_value.startswith("hermes."):
                response["control_scope"] = HERMES_CONTROL_SCOPE
            return response
        except OperatorControlError as exc:
            reason = exc.code
        except CanaryBlocked as exc:
            reason = str(exc).strip() or "CANARY_BLOCKED"
        except Exception as exc:
            reason = type(exc).__name__.upper()
        self._finish_action(
            action_id,
            status="FAILED",
            reason=reason,
            result={"ok": False, "reason": reason},
        )
        failure = {
            "ok": False,
            "action": requested_action_value,
            "target": target_value,
            "action_id": action_id,
            "action_status": "FAILED" if action_id else None,
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
    "HERMES_EXTERNAL_STATUS",
    "HERMES_EXTERNAL_EVIDENCE",
    "CANARY_CONNECTIVITY_CONFIG_KEY",
    "HermesOperatorAdapter",
    "OperatorControlError",
    "OperatorControlPlane",
]
