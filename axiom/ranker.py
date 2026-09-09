"""Deterministic, persisted prediction-candidate canary selection."""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
from typing import Any, Mapping

from .canary import (
    CanaryService,
    _canary_qualification_hash,
    _canary_qualification_projection,
    _canary_lifecycle_snapshot_hashes,
    _canary_ranking_snapshot_hash,
    _canary_prediction_market,
    _canary_scope_binding,
    _canary_current_scope_resolution,
    _CANARY_RANKING_EVIDENCE_ALIASES,
)
from .domain import ensure_utc, utc_now

from .storage import AxiomStore
from .lifecycle import (
    CandidateLifecycleManager,
    CandidateStage,
    _canonical_scope_gate_error,
)
from .data_quality import evaluate_prediction_data_quality, persisted_quality_fields


class _DatasetEvidenceChanged(RuntimeError):
    """Raised when an attested dataset changes during ranking prevalidation."""

    error_code = "DATASET_EVIDENCE_CHANGED"

class _LifecycleEvidenceChanged(RuntimeError):
    """Raised when a lifecycle snapshot changes during ranking prevalidation."""

    error_code = "LIFECYCLE_SNAPSHOT_CHANGED"

class CandidateCanaryRanker:
    """Continuously evaluate and rank frozen prediction candidates.

    Ranking reads only immutable validation evidence and the optional paper
    forward evidence already persisted on the candidate.  The locked holdout
    partition is intentionally not read by this class.
    """
    FORMULA_VERSION = "validation-weighted-v2-fidelity"
    # score = sum(weight * component) / sum(weight for available components).
    # Fidelity is a separate weighted component; PRICE_PROXY is never treated
    # as timestamped depth and cannot compensate for failed gates.
    WEIGHTS = {
        "expectancy": 0.18,
        "confidence_lower_bound": 0.14,
        "robustness": 0.14,
        "sample": 0.10,
        "execution_feasibility": 0.10,
        "historical_data_integrity": 0.10,
        "execution_fidelity_score": 0.08,
        "calibration": 0.06,
        "drawdown": 0.03,
        "liquidity": 0.02,
        "forward_expectancy": 0.05,
    }
    _STAGES = frozenset({"FROZEN", "PAPER_FORWARD", "PAPER_PROMOTABLE"})
    _QUALITY_SCORES = {
        "HIGH": 1.0,
        "MEDIUM": 0.5,
        "LOW": 0.0,
        "ORDER_BOOK_SIMULATED": 1.0,
        "OHLCV_SIMULATED": 0.75,
        "PRICE_PROXY": 0.5,
        "MODEL_ESTIMATE": 0.5,
    }

    def __init__(
        self,
        store: AxiomStore,
        *,
        service: CanaryService | None = None,
        clock=utc_now,
    ) -> None:
        self.store = store
        self.service = service or CanaryService(store, clock=clock)
        self.clock = clock

    @staticmethod
    def _number(value: Any) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        return result if math.isfinite(result) else None

    @classmethod
    def _value(cls, payload: Mapping[str, Any], *names: str) -> Any:
        validation = payload.get("validation")
        nested = validation if isinstance(validation, Mapping) else {}
        for name in names:
            if name in payload:
                return payload[name]
            if name in nested:
                return nested[name]
        return None

    @classmethod
    def _prediction_market(cls, payload: Mapping[str, Any]) -> str | None:
        return _canary_prediction_market(payload)

    @classmethod
    def _sample_count(cls, payload: Mapping[str, Any]) -> int | None:
        value = cls._value(payload, *_CANARY_RANKING_EVIDENCE_ALIASES["sample_count"])
        if value is None:
            minimum = payload.get("minimum_sample_check")
            if isinstance(minimum, Mapping):
                value = minimum.get("count", minimum.get("observations"))
        try:
            number = int(value)
        except (TypeError, ValueError):
            return None
        return number if number >= 0 else None

    @classmethod
    def _trade_count(cls, payload: Mapping[str, Any]) -> int | None:
        value = cls._value(payload, *_CANARY_RANKING_EVIDENCE_ALIASES["trade_count"])
        if value is None:
            minimum = payload.get("minimum_sample_check")
            if isinstance(minimum, Mapping):
                value = minimum.get("trades")
        try:
            number = int(value)
        except (TypeError, ValueError):
            return None
        return number if number >= 0 else None

    @classmethod
    def _quality(cls, payload: Mapping[str, Any], *names: str) -> float | None:
        value = cls._value(payload, *names)
        if isinstance(value, Mapping):
            value = value.get("label", value.get("quality", value.get("score")))
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, str):
            return cls._QUALITY_SCORES.get(value.strip().upper())
        return cls._number(value)

    @staticmethod
    def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
        return max(lower, min(upper, value))

    @classmethod
    def _score(cls, value: float, *, midpoint: float = 0.0, scale: float = 1.0) -> float:
        return cls._clamp(0.5 + (value - midpoint) / scale)

    @classmethod
    def _cluster_key(cls, candidate_id: str, payload: Mapping[str, Any], versions: Mapping[str, Any]) -> str:
        explicit = payload.get("mutation_cluster") or payload.get("cluster_key")
        lineage = payload.get("lineage")
        if isinstance(lineage, (list, tuple)) and lineage:
            root = str(lineage[0])
        else:
            root = str(payload.get("root_candidate_id") or payload.get("parent_id") or candidate_id)
        family = str(payload.get("experiment_family") or payload.get("family") or "")
        material = {
            "explicit_cluster": str(explicit) if explicit is not None else "",
            "root": root,
            "family": family,
            "dataset_id": versions.get("dataset_id"),
            "dataset_version": versions.get("dataset_version"),
            "plan_hash": versions.get("plan_hash"),
            "market_scope_hash": versions.get("market_scope_hash"),
            "market_scope_version": versions.get("market_scope_version"),
            "strategy_hash": versions.get("strategy_hash"),
            "model_hash": versions.get("model_hash"),
            "config_hash": versions.get("config_hash"),
        }
        encoded = json.dumps(material, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return "cluster-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:20]

    @classmethod
    def _versions(cls, payload: Mapping[str, Any], frozen_hash: str) -> dict[str, Any]:
        plan = payload.get("experiment_plan") if isinstance(payload.get("experiment_plan"), Mapping) else {}
        scope = _canary_scope_binding(payload)
        return {
            "frozen_hash": frozen_hash,
            "plan_hash": scope.get("plan_hash") or str(payload.get("plan_hash") or ""),
            "market_scope_hash": scope.get("scope_hash") or "",
            "market_scope_version": scope.get("scope_version") or "",
            "strategy_hash": str(payload.get("strategy_hash") or ""),
            "model_hash": str(payload.get("model_hash") or ""),
            "config_hash": str(payload.get("config_hash") or ""),
            "dataset_id": str(payload.get("dataset_id") or plan.get("dataset_id") or ""),
            "dataset_version": str(payload.get("dataset_version") or plan.get("dataset_version") or ""),
            "dataset_selector": dict(scope.get("dataset_selector") or {}),
            "dataset_attestation": dict(scope.get("dataset_attestation") or {}),
            "forward_test_id": str(payload.get("forward_test_id") or ""),
            "locked_holdout_used": False,
            "formula_version": cls.FORMULA_VERSION,
        }

    @classmethod
    def _rank_evidence(
        cls,
        payload: Mapping[str, Any],
        frozen_hash: str | None,
        quality: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str | None, dict[str, Any]]:
        versions = cls._versions(payload, str(frozen_hash or ""))
        if not frozen_hash:
            return None, "FROZEN_HASH_MISSING", versions
        legacy_quality = cls._quality(
            payload, *_CANARY_RANKING_EVIDENCE_ALIASES["quality"]
        )
        if isinstance(quality, Mapping) and quality.get("applicable"):
            integrity_score = (
                1.0 if quality.get("historical_data_integrity_passed") else None
            )
            fidelity_score = cls._number(quality.get("historical_execution_fidelity_score"))
        else:
            integrity_score = legacy_quality
            fidelity_score = cls._number(
                payload.get(
                    _CANARY_RANKING_EVIDENCE_ALIASES["execution_fidelity_score"][0],
                    legacy_quality,
                )
            )
        required = {
            "expectancy": cls._number(
                cls._value(payload, *_CANARY_RANKING_EVIDENCE_ALIASES["expectancy"])
            ),
            "confidence_lower_bound": cls._number(
                cls._value(
                    payload,
                    *_CANARY_RANKING_EVIDENCE_ALIASES["confidence_lower_bound"],
                )
            ),
            "robustness": cls._number(
                cls._value(payload, *_CANARY_RANKING_EVIDENCE_ALIASES["stability"])
            ),
            "calibration": cls._number(
                cls._value(payload, *_CANARY_RANKING_EVIDENCE_ALIASES["calibration"])
            ),
            "sample_count": cls._sample_count(payload),
            "trade_count": cls._trade_count(payload),
            "execution_feasibility": cls._quality(
                payload, *_CANARY_RANKING_EVIDENCE_ALIASES["execution_quality"]
            ),
            "historical_data_integrity": integrity_score,
            "execution_fidelity_score": fidelity_score,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            return None, "RANKING_EVIDENCE_MISSING:" + ",".join(missing), versions
        if required["trade_count"] <= 0:
            return None, "RANKING_EVIDENCE_ZERO_TRADES", versions
        plan = payload.get("experiment_plan") if isinstance(payload.get("experiment_plan"), Mapping) else {}
        min_samples = cls._number(plan.get("min_independent_samples", plan.get("min_samples", 30))) or 30.0
        min_trades = cls._number(plan.get("min_trades", 0)) or 0.0
        if required["sample_count"] < min_samples or required["trade_count"] < min_trades:
            return None, "RANKING_EVIDENCE_BELOW_MINIMUM_SAMPLE", versions
        forward_evidence = payload.get("forward_evidence")
        forward_expectancy = cls._number(
            payload.get(_CANARY_RANKING_EVIDENCE_ALIASES["forward_expectancy"][0])
        )
        if forward_expectancy is None and isinstance(forward_evidence, Mapping):
            forward_expectancy = cls._number(forward_evidence.get("forward_expectancy"))
        components: dict[str, float] = {
            "expectancy": cls._score(float(required["expectancy"])),
            "confidence_lower_bound": cls._score(float(required["confidence_lower_bound"])),
            "robustness": cls._clamp(float(required["robustness"])),
            "sample": cls._clamp(float(required["sample_count"]) / max(1.0, min_samples * 2.0)),
            "execution_feasibility": cls._clamp(float(required["execution_feasibility"])),
            "historical_data_integrity": cls._clamp(float(required["historical_data_integrity"])),
            "execution_fidelity_score": cls._clamp(float(required["execution_fidelity_score"])),
            "calibration": cls._clamp(float(required["calibration"])),
        }
        optional: dict[str, float] = {}
        drawdown = cls._number(
            cls._value(payload, *_CANARY_RANKING_EVIDENCE_ALIASES["max_drawdown"])
        )
        if drawdown is not None:
            optional["drawdown"] = cls._clamp(1.0 - drawdown)
        liquidity = cls._number(
            cls._value(payload, *_CANARY_RANKING_EVIDENCE_ALIASES["liquidity"])
        )
        if liquidity is not None:
            optional["liquidity"] = cls._clamp(liquidity)
        if forward_expectancy is not None:
            optional["forward_expectancy"] = cls._score(forward_expectancy)
        components.update(optional)
        weights = {key: cls.WEIGHTS[key] for key in components}
        total_weight = sum(weights.values())
        score = sum(components[key] * weights[key] for key in components) / total_weight
        fidelity_penalty = 1.0 - float(components["execution_fidelity_score"])
        evidence = {
            "raw": {
                "validation_expectancy": required["expectancy"],
                "validation_confidence_lower_bound": required["confidence_lower_bound"],
                "validation_stability": required["robustness"],
                "validation_calibration": required["calibration"],
                "validation_sample_count": required["sample_count"],
                "validation_trade_count": required["trade_count"],
                "validation_execution_quality": required["execution_feasibility"],
                "historical_data_integrity": required["historical_data_integrity"],
                "historical_execution_fidelity": (
                    quality.get("historical_execution_fidelity")
                    if isinstance(quality, Mapping) else payload.get("historical_execution_fidelity")
                ),
                "fidelity_penalty": fidelity_penalty,
                "execution_fidelity_score": required["execution_fidelity_score"],
                "validation_max_drawdown": drawdown,
                "validation_liquidity": liquidity,
                "forward_expectancy": forward_expectancy,
            },
            "fidelity_penalty": fidelity_penalty,
            "components": components,
            "weights": weights,
            "total_score": score,
            "versions": versions,
        }
        return evidence, None, versions

    def _candidate_records(self) -> list[Mapping[str, Any]]:
        records = self.store.load_candidate_lifecycle(limit=10000)
        return [item for item in records if isinstance(item, Mapping)] if isinstance(records, list) else []
    def eligible_scan_rows(
        self,
        *,
        now: datetime | None = None,
        ranking_run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return eligible candidates even when no positive ranking exists.

        Autonomous signal coverage is intentionally independent from ranking
        evidence.  A candidate can pass immutable qualification while its
        validation metrics are absent or below the ranking minimum; those
        candidates still need a durable bounded signal check.
        """
        timestamp = ensure_utc(now or self.clock())
        result: list[dict[str, Any]] = []
        for record in self._candidate_records():
            candidate_id = str(record.get("candidate_id") or "").strip()
            if (
                not candidate_id
                or str(record.get("stage") or "") not in self._STAGES
            ):
                continue
            payload = self.service._merged_lifecycle_payload(record)
            if not isinstance(payload, Mapping):
                continue
            if _canonical_scope_gate_error(
                str(record.get("stage") or ""),
                payload,
            ) is not None:
                continue
            if self._prediction_market(payload) not in {
                "prediction",
                "polymarket",
                "prediction_market",
            }:
                continue
            validation = self.service.validate_eligibility(
                candidate_id,
                _record=record,
                _verify_attestation=True,
            )
            if not validation.get("eligible"):
                continue
            binding = validation.get("binding")
            binding_reason = (
                str(binding.get("reason_code") or "").strip().upper()
                if isinstance(binding, Mapping)
                else ""
            )
            if (
                isinstance(binding, Mapping)
                and (
                    binding.get("reevaluation_required")
                    or (
                        not binding.get("bound")
                        and binding_reason not in {"", "ELIGIBILITY_MISSING"}
                    )
                )
            ):
                continue
            frozen_hash = str(
                validation.get("frozen_hash")
                or self.service._lifecycle_frozen_hash(record)
                or ""
            ).strip()
            qualification_hash = str(
                (
                    binding.get("qualification_hash")
                    if isinstance(binding, Mapping)
                    else None
                )
                or validation.get("qualification_hash")
                or ""
            ).strip()
            if not frozen_hash or not qualification_hash:
                continue
            quality = validation.get("data_quality")
            ranking_snapshot_hash = _canary_ranking_snapshot_hash(
                candidate_id,
                str(record.get("stage") or ""),
                payload,
                qualification_hash=qualification_hash,
                quality=quality if isinstance(quality, Mapping) else None,
            )
            versions = self._versions(payload, frozen_hash)
            result.append(
                {
                    "candidate_id": candidate_id,
                    "ranking_run_id": ranking_run_id,
                    "ranking_timestamp": timestamp.isoformat(),
                    "rank": 0,
                    "total_score": None,
                    "component_scores": {},
                    "evidence_versions": versions,
                    "cluster_key": self._cluster_key(
                        candidate_id,
                        payload,
                        versions,
                    ),
                    "cluster_representative": 1,
                    "selected": 0,
                    "reason": "RANKING_EVIDENCE_MISSING",
                    "qualification_hash": qualification_hash,
                    "ranking_snapshot_hash": ranking_snapshot_hash,
                }
            )
        result.sort(key=lambda row: str(row.get("candidate_id") or ""))
        return result

    def _candidate_inventory_token(
        self,
        records: list[Mapping[str, Any]],
    ) -> str:
        """Hash the ranker-visible lifecycle evidence fence.

        Mutable timestamps and full JSON payloads are deliberately excluded:
        signal telemetry and harmless counters must not restart an in-progress
        scan.  Stage, frozen binding, dataset attestation, and the canonical
        qualification/ranking hashes remain fenced.
        """
        inventory: list[dict[str, Any]] = []
        for record in records:
            stage = str(record.get("stage") or "")
            candidate_id = str(record.get("candidate_id") or "").strip()
            if not candidate_id or stage not in self._STAGES:
                continue
            payload = self.service._merged_lifecycle_payload(record)
            if not isinstance(payload, Mapping):
                continue
            _, qualification_hash, ranking_snapshot_hash = (
                _canary_lifecycle_snapshot_hashes(self.store, record)
            )
            inventory.append(
                {
                    "candidate_id": candidate_id,
                    "stage": stage,
                    "frozen_hash": self.service._lifecycle_frozen_hash(record),
                    "attestation": self._dataset_attestation_snapshot(payload),
                    "qualification_hash": qualification_hash,
                    "ranking_snapshot_hash": ranking_snapshot_hash,
                }
            )
        inventory.sort(
            key=lambda item: (
                str(item["candidate_id"]),
                str(item["stage"]),
                str(item["frozen_hash"]),
                str(item["qualification_hash"]),
                str(item["ranking_snapshot_hash"]),
                json.dumps(item["attestation"], sort_keys=True),
            )
        )
        encoded = json.dumps(
            inventory,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    def _dataset_attestation_snapshot(
        self,
        payload: Mapping[str, Any],
        *,
        connection: Any | None = None,
    ) -> dict[str, Any]:
        """Read the exact durable attestation bound to a prediction dataset."""
        dataset_id = str(payload.get("dataset_id") or "").strip()
        dataset_version = str(payload.get("dataset_version") or "").strip()
        snapshot: dict[str, Any] = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "present": False,
            "status": None,
            "policy_version": None,
            "attestation_hash": None,
        }
        if not dataset_id or not dataset_version:
            return snapshot
        if connection is None:
            with self.store._lock:
                row = self.store.connection.execute(
                    "SELECT status,policy_version,attestation_hash "
                    "FROM dataset_integrity_attestation "
                    "WHERE dataset_id=? AND dataset_version=?",
                    (dataset_id, dataset_version),
                ).fetchone()
        else:
            row = connection.execute(
                "SELECT status,policy_version,attestation_hash "
                "FROM dataset_integrity_attestation "
                "WHERE dataset_id=? AND dataset_version=?",
                (dataset_id, dataset_version),
            ).fetchone()
        if row is not None:
            snapshot.update(
                {
                    "present": True,
                    "status": row["status"],
                    "policy_version": row["policy_version"],
                    "attestation_hash": row["attestation_hash"],
                }
            )
        return snapshot


    def _persist_quality_projection(
        self,
        record: Mapping[str, Any],
        payload: Mapping[str, Any],
        quality: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        """Keep qualification reads side-effect free."""
        return record, payload


    def _snapshot_hashes(
        self,
        record: Mapping[str, Any],
        payload: Mapping[str, Any],
        quality: Mapping[str, Any],
        frozen_hash: str,
    ) -> tuple[dict[str, Any], str, str] | None:
        # The lifecycle row may retain forward metrics both nested under
        # ``forward_evidence`` and at the top level.  Hash the same merged
        # projection used by the final commit fence so duplicate placement is
        # representation-only, while conflicting values remain material.
        canonical_payload = self.service._merged_lifecycle_payload(record)
        if not isinstance(canonical_payload, Mapping):
            return None
        payload = canonical_payload
        qualification = _canary_qualification_projection(
            str(record.get("candidate_id") or ""),
            payload,
            frozen_hash=frozen_hash,
            quality=quality,
        )
        qualification_hash = _canary_qualification_hash(qualification)
        ranking_snapshot_hash = _canary_ranking_snapshot_hash(
            str(record.get("candidate_id") or ""),
            str(record.get("stage") or ""),
            payload,
            qualification_hash=qualification_hash,
            quality=quality,
        )
        if not qualification_hash or not ranking_snapshot_hash:
            return None
        return qualification, qualification_hash, ranking_snapshot_hash
    @staticmethod
    def _rankless_persisted_row(row: Mapping[str, Any]) -> bool:
        if not isinstance(row, Mapping):
            return False
        if str(row.get("cluster_key") or "").strip():
            return False
        try:
            rank = int(row.get("rank"))
            representative = int(row.get("cluster_representative"))
        except (TypeError, ValueError, OverflowError):
            return False
        if (
            rank != 0
            or representative != 0
            or row.get("total_score") is not None
        ):
            return False
        reason = str(row.get("reason") or "").strip().upper()
        return (
            reason.startswith("RANKING_EVIDENCE_MISSING")
            or reason in {
                "RANKING_EVIDENCE_BELOW_MINIMUM_SAMPLE",
                "RANKING_EVIDENCE_ZERO_TRADES",
                "FROZEN_HASH_MISSING",
            }
        )

    def validate_persisted_ranking(
        self,
        row: Mapping[str, Any],
        *,
        ranking_run_id: str,
        now: datetime,
    ) -> bool:
        """Reject ranking rows that are not part of the current fenced run."""
        if not isinstance(row, Mapping):
            return False
        candidate_id = str(row.get("candidate_id") or "").strip()
        if not candidate_id or str(row.get("ranking_run_id") or "") != str(ranking_run_id):
            return False
        try:
            stamp = ensure_utc(datetime.fromisoformat(str(row.get("ranking_timestamp"))))
            age = (ensure_utc(now) - stamp).total_seconds()
            rank = int(row.get("rank"))
            raw_score = row.get("total_score")
            score = float(raw_score) if raw_score is not None else None
            representative = int(row.get("cluster_representative"))
        except (TypeError, ValueError, OverflowError):
            return False
        rankless = self._rankless_persisted_row(row)
        reason = str(row.get("reason") or "").strip().upper()
        if (
            age < 0
            or age > 60.0
            or (score is not None and not math.isfinite(score))
            or (score is None and not rankless)
            or rank < 0
            or representative not in (0, 1)
            or (
                rank == 0
                and (
                    representative != 0
                    or (
                        reason != "DIVERSITY_CLUSTER_NON_REPRESENTATIVE"
                        and not rankless
                    )
                )
            )
        ):
            return False
        cluster_key = str(row.get("cluster_key") or "").strip()
        if not cluster_key and not rankless:
            return False
        versions = row.get("evidence_versions")
        if not isinstance(versions, Mapping):
            raw_versions = row.get("evidence_versions_json")
            try:
                versions = json.loads(str(raw_versions or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                versions = None
        if not isinstance(versions, Mapping) or not versions:
            return False
        components = row.get("component_scores")
        if not isinstance(components, Mapping):
            raw_components = row.get("component_scores_json")
            try:
                components = json.loads(str(raw_components or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                components = None
        if not isinstance(components, Mapping):
            return False
        lifecycle = self.store.load_candidate_lifecycle(candidate_id)
        if not isinstance(lifecycle, Mapping) or str(lifecycle.get("stage") or "") not in self._STAGES:
            return False
        payload = self.service._merged_lifecycle_payload(lifecycle)
        if not isinstance(payload, Mapping):
            return False
        if self._prediction_market(payload) not in {
            "prediction",
            "polymarket",
            "prediction_market",
        }:
            return False
        scope_error = _canonical_scope_gate_error(
            str(lifecycle.get("stage") or ""),
            payload,
        )
        if scope_error is not None:
            return False
        scope_binding = _canary_scope_binding(payload)
        scope_resolution = _canary_current_scope_resolution(
            self.store,
            candidate_id,
            payload,
            now=ensure_utc(now),
        )
        if not scope_resolution.get("bound"):
            return False
        eligibility = self.store.connection.execute(
            "SELECT candidate_id,frozen_hash,evidence_json "
            "FROM canary_eligibility WHERE candidate_id=?",
            (candidate_id,),
        ).fetchone()
        binding = self.service._eligibility_binding_result(
            candidate_id,
            eligibility,
            record=lifecycle,
            verify_attestation=False,
        )
        if not binding.get("bound") or binding.get("reevaluation_required"):
            return False
        if not self.service.validate_eligibility(
            candidate_id,
            _record=lifecycle,
            _verify_attestation=False,
        ).get("eligible"):
            return False
        quality = evaluate_prediction_data_quality(
            self.store,
            payload,
            verify_attestation=False,
        )
        expected_hash = _canary_ranking_snapshot_hash(
            candidate_id,
            str(lifecycle.get("stage") or ""),
            payload,
            qualification_hash=str(binding.get("qualification_hash") or ""),
            quality=quality,
        )
        expected_versions = self._versions(
            payload,
            str(self.service._lifecycle_frozen_hash(lifecycle) or ""),
        )
        return bool(
            row.get("qualification_hash")
            and row.get("qualification_hash") == binding.get("qualification_hash")
            and row.get("ranking_snapshot_hash")
            and row.get("ranking_snapshot_hash") == expected_hash
            and str(versions.get("frozen_hash") or "")
            == str(self.service._lifecycle_frozen_hash(lifecycle) or "")
            and str(versions.get("plan_hash") or "")
            == str(expected_versions.get("plan_hash") or "")
            and str(versions.get("market_scope_hash") or "")
            == str(expected_versions.get("market_scope_hash") or "")
            and str(versions.get("market_scope_version") or "")
            == str(expected_versions.get("market_scope_version") or "")
        )
    def evaluate_and_select(self, now: datetime | None = None) -> dict[str, Any]:
        expected_projection_version = self.service._readiness_projection_version()
        try:
            return self._evaluate_and_select(now)
        except Exception as exc:
            error_code = getattr(exc, "error_code", None)
            if not isinstance(error_code, str) or not error_code:
                error_code = str(type(exc).__name__).upper()[:64] or "UNKNOWN"
            self.service._persist_evaluation_failure(
                error_code=error_code,
                expected_projection_version=expected_projection_version,
            )
            raise

    def _evaluate_and_select(self, now: datetime | None = None) -> dict[str, Any]:
        timestamp = ensure_utc(now or self.clock())
        candidates: list[dict[str, Any]] = []
        invalidated_reasons: dict[str, str] = {}
        prevalidated: list[dict[str, Any]] = []
        inventory = self._candidate_records()
        # Keep the initial membership fence separate from the mutable
        # evidence token.  Projection/eligibility preparation may intentionally
        # rewrite lifecycle evidence, but an externally added or removed
        # candidate must still fail closed.
        inventory_candidate_ids = frozenset(
            str(record.get("candidate_id") or "").strip()
            for record in inventory
            if str(record.get("candidate_id") or "").strip()
        )
        verified_datasets: set[tuple[str, str]] = set()
        for original_record in inventory:
            candidate_id = str(original_record.get("candidate_id") or "").strip()
            if not candidate_id:
                continue
            stage = str(original_record.get("stage") or "")
            if stage == CandidateStage.REJECTED.value:
                invalidated_reasons[candidate_id] = "LIFECYCLE_REJECTED"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            payload = self.service._merged_lifecycle_payload(original_record)
            if not isinstance(payload, Mapping):
                invalidated_reasons[candidate_id] = "ELIGIBILITY_INVALID"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            scope_error = _canonical_scope_gate_error(stage, payload)
            if scope_error is not None:
                invalidated_reasons[candidate_id] = scope_error
                self.service.invalidate_eligibility(
                    candidate_id,
                    scope_error,
                    publish_readiness=False,
                )
                continue
            attestation = self._dataset_attestation_snapshot(payload)
            dataset_identity = (
                str(attestation.get("dataset_id") or ""),
                str(attestation.get("dataset_version") or ""),
            )
            if (
                attestation.get("status") != "CURRENT"
                and all(dataset_identity)
                and dataset_identity not in verified_datasets
            ):
                verified_datasets.add(dataset_identity)
                try:
                    self.store.verify_dataset_integrity_attestation(*dataset_identity)
                except Exception:
                    pass
                attestation = self._dataset_attestation_snapshot(payload)
            quality = evaluate_prediction_data_quality(self.store, payload)
            quality_status = (
                quality.get("dataset_integrity_attestation_status")
                if isinstance(quality, Mapping)
                else None
            )
            quality_hash = (
                quality.get("dataset_integrity_attestation_hash")
                if isinstance(quality, Mapping)
                else None
            )
            if (
                attestation["status"] != quality_status
                or attestation["attestation_hash"] != quality_hash
            ):
                invalidated_reasons[candidate_id] = "DATASET_EVIDENCE_CHANGED"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            record, payload = self._persist_quality_projection(
                original_record,
                payload,
                quality,
            )
            if self._prediction_market(payload) not in {
                "prediction",
                "polymarket",
                "prediction_market",
            }:
                invalidated_reasons[candidate_id] = "PREDICTION_MARKET_ONLY"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            validation = self.service.validate_eligibility(
                candidate_id,
                _record=record,
            )
            binding = validation.get("binding")
            binding_reason = (
                binding.get("reason_code")
                if isinstance(binding, Mapping)
                else None
            )
            binding_missing = binding_reason == "ELIGIBILITY_MISSING"
            binding_bound = (
                isinstance(binding, Mapping) and bool(binding.get("bound"))
            )
            if not validation.get("eligible") or (
                not binding_bound and not binding_missing
            ):
                normalized_binding_reason = str(binding_reason or "").strip().upper()
                precise_reason = (
                    normalized_binding_reason
                    if normalized_binding_reason
                    and normalized_binding_reason != "ELIGIBILITY_MISSING"
                    else ""
                )
                quality_reasons = (
                    validation.get("data_quality", {}).get("reasons")
                    if isinstance(validation.get("data_quality"), Mapping)
                    else ()
                )
                if not precise_reason and isinstance(quality_reasons, (list, tuple)):
                    precise_reason = next(
                        (
                            str(reason).strip().upper()
                            for reason in quality_reasons
                            if str(reason).strip()
                        ),
                        "",
                    )
                if not precise_reason:
                    precise_reason = str(
                        validation.get("reason_code") or ""
                    ).strip().upper()
                invalidated_reasons[candidate_id] = (
                    precise_reason or "ELIGIBILITY_INVALID"
                )
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            prevalidated.append(
                {
                    "candidate_id": candidate_id,
                    "record": record,
                    "payload": payload,
                    "quality": quality,
                    "validation": validation,
                    "attestation": attestation,
                }
            )

        candidates: list[dict[str, Any]] = []
        for prepared in prevalidated:
            candidate_id = str(prepared["candidate_id"])
            # Keep the original prevalidation record as the ranking baseline.
            # Re-reading here could absorb an A/B lifecycle mutation that
            # happened while ranking evidence was being prepared, defeating
            # the fenced commit's change detection.
            record = prepared["record"]
            payload = prepared["payload"]
            if not isinstance(record, Mapping) or not isinstance(payload, Mapping):
                invalidated_reasons[candidate_id] = "ELIGIBILITY_INVALID"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            quality = prepared["quality"]
            validation = prepared["validation"]
            attestation = prepared["attestation"]
            frozen_hash = str(validation.get("frozen_hash") or "")
            hashes = self._snapshot_hashes(
                record,
                payload,
                quality,
                frozen_hash,
            )
            if hashes is None:
                invalidated_reasons[candidate_id] = "ELIGIBILITY_INVALID"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            _, qualification_hash, ranking_snapshot_hash = hashes
            validation_qualification = validation.get("qualification")
            validation_hash = validation.get("qualification_hash")
            eligibility_frozen_hash = str(validation.get("frozen_hash") or "")
            if (
                not isinstance(validation_qualification, Mapping)
                or not isinstance(validation_hash, str)
                or not validation_hash
                or validation_hash != qualification_hash
                or not eligibility_frozen_hash
            ):
                invalidated_reasons[candidate_id] = "ELIGIBILITY_INVALID"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            try:
                # Always stage canonical qualification evidence derived from
                # the current lifecycle. A legacy full-payload binding is
                # accepted only after re-evaluation and is rewritten before
                # the ranking commit, so the final fence observes the same
                # evidence it will persist.
                eligibility_evidence_json = json.dumps(
                    {
                        **dict(validation_qualification),
                        "qualification_hash": validation_hash,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            except (TypeError, ValueError):
                invalidated_reasons[candidate_id] = "ELIGIBILITY_INVALID"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                    publish_readiness=False,
                )
                continue
            evidence, reason, versions = self._rank_evidence(
                payload,
                frozen_hash,
                quality=quality,
            )
            versions.update(
                {
                    "qualification_hash": qualification_hash,
                    "ranking_snapshot_hash": ranking_snapshot_hash,
                    "dataset_integrity_attestation_status": attestation["status"],
                    "dataset_integrity_attestation_policy_version": attestation[
                        "policy_version"
                    ],
                    "dataset_integrity_attestation_hash": attestation[
                        "attestation_hash"
                    ],
                }
            )
            item: dict[str, Any] = {
                "candidate_id": candidate_id,
                "payload": dict(payload),
                "record": record,
                "quality": quality,
                "evidence": evidence,
                "reason": reason or "RANKING_EVIDENCE_MISSING",
                "versions": versions,
                "qualification_hash": qualification_hash,
                "ranking_snapshot_hash": ranking_snapshot_hash,
                "eligibility_frozen_hash": eligibility_frozen_hash,
                "eligibility_evidence_json": eligibility_evidence_json,
                "attestation": attestation,
            }
            if evidence is not None:
                item["reason"] = ""
                item["cluster_key"] = self._cluster_key(
                    candidate_id,
                    payload,
                    versions,
                )
            candidates.append(item)

        # Persist ranker's eligibility attestations only after the evidence
        # preparation fence.  A concurrent C/D telemetry update can then
        # leave the prior CURRENT readiness reason untouched until release;
        # A/B changes remain fenced by the final lifecycle hash checks.
        self.service._batch_mark_eligible_prevalidated(prevalidated)

        changed_ids: set[str] = set()
        stable: list[dict[str, Any]] = []
        with self.store._lock:
            connection = self.store.connection
            if connection.in_transaction:
                raise RuntimeError("ranking transaction already active")
            # Capture the mutable evidence baseline only after all intentional
            # projection, eligibility, and candidate preparation writes have
            # completed.  The transaction re-reads the same persisted records
            # so external A/B changes between these fences still abort.
            inventory_token = self._candidate_inventory_token(self._candidate_records())
            connection.execute("BEGIN IMMEDIATE")
            try:
                current_inventory = self._candidate_records()
                current_candidate_ids = frozenset(
                    str(record.get("candidate_id") or "").strip()
                    for record in current_inventory
                    if str(record.get("candidate_id") or "").strip()
                )
                if (
                    current_candidate_ids != inventory_candidate_ids
                    or self._candidate_inventory_token(current_inventory)
                    != inventory_token
                ):
                    raise _LifecycleEvidenceChanged(
                        "candidate inventory changed during ranking"
                    )
                for item in candidates:
                    candidate_id = str(item["candidate_id"])
                    current = self.store.load_candidate_lifecycle(candidate_id)
                    if not isinstance(current, Mapping):
                        raise _LifecycleEvidenceChanged(
                            f"lifecycle snapshot disappeared for {candidate_id}"
                        )
                    current_payload = self.service._merged_lifecycle_payload(current)
                    if not isinstance(current_payload, Mapping):
                        raise _LifecycleEvidenceChanged(
                            f"lifecycle payload disappeared for {candidate_id}"
                        )
                    current_frozen_hash = self.service._lifecycle_frozen_hash(current)
                    expected_frozen_hash = str(
                        item["versions"].get("frozen_hash") or ""
                    )
                    quality = item.get("quality")
                    expected_attestation = item.get("attestation")
                    attestation = self._dataset_attestation_snapshot(
                        current_payload,
                        connection=connection,
                    )
                    attestation_changed = (
                        not isinstance(expected_attestation, Mapping)
                        or attestation != expected_attestation
                    )
                    stage_changed = (
                        str(current.get("stage") or "")
                        != str(item["record"].get("stage") or "")
                    )
                    frozen_changed = current_frozen_hash != expected_frozen_hash
                    if attestation_changed:
                        raise _DatasetEvidenceChanged(
                            f"dataset attestation changed for {candidate_id}"
                        )
                    if stage_changed or frozen_changed:
                        raise _LifecycleEvidenceChanged(
                            f"lifecycle binding changed for {candidate_id}"
                        )
                    # Historical evidence and eligibility were fully computed
                    # before BEGIN IMMEDIATE.  The fenced section only checks
                    # lifecycle, attestation, and ranking hashes before writing.
                    current_hashes = self._snapshot_hashes(
                        current,
                        current_payload,
                        quality,
                        current_frozen_hash,
                    )
                    if current_hashes is None:
                        changed_ids.add(candidate_id)
                        continue
                    # A ranking-input mutation (for example, forward
                    # expectancy or duration) invalidates the entire fenced
                    # run.  It is not safe to publish a mixed-time ranking.
                    # C/D telemetry and harmless metadata are intentionally
                    # absent from this hash and therefore continue normally.
                    if current_hashes[2] != item["ranking_snapshot_hash"]:
                        raise _LifecycleEvidenceChanged(
                            f"ranking snapshot changed for {candidate_id}"
                        )
                    eligibility_attestation = connection.execute(
                        "SELECT candidate_id,frozen_hash,evidence_json "
                        "FROM canary_eligibility WHERE candidate_id=?",
                        (candidate_id,),
                    ).fetchone()
                    if (
                        eligibility_attestation is None
                        or str(eligibility_attestation["candidate_id"] or "").strip()
                        != candidate_id
                        or eligibility_attestation["frozen_hash"]
                        != item["eligibility_frozen_hash"]
                        or eligibility_attestation["evidence_json"]
                        != item["eligibility_evidence_json"]
                        or current_hashes[1] != item["qualification_hash"]
                    ):
                        changed_ids.add(candidate_id)
                        continue
                    stable.append(item)

                ranked = [item for item in stable if item["evidence"] is not None]
                ranked.sort(
                    key=lambda item: (
                        -float(item["evidence"]["total_score"]),
                        -float(
                            item["evidence"]["raw"][
                                "validation_confidence_lower_bound"
                            ]
                        ),
                        -float(item["evidence"]["raw"]["validation_expectancy"]),
                        str(item["candidate_id"]),
                    )
                )
                representatives: dict[str, dict[str, Any]] = {}
                for item in ranked:
                    representatives.setdefault(str(item["cluster_key"]), item)
                selected_representatives = sorted(
                    representatives.values(),
                    key=lambda item: (
                        -float(item["evidence"]["total_score"]),
                        -float(
                            item["evidence"]["raw"][
                                "validation_confidence_lower_bound"
                            ]
                        ),
                        -float(item["evidence"]["raw"]["validation_expectancy"]),
                        str(item["candidate_id"]),
                    ),
                )
                rank_by_id = {
                    str(item["candidate_id"]): index
                    for index, item in enumerate(selected_representatives, start=1)
                }
                selected_id = (
                    str(selected_representatives[0]["candidate_id"])
                    if selected_representatives
                    else None
                )
                persisted: list[dict[str, Any]] = []
                fingerprint_rows: list[dict[str, Any]] = []
                for item in stable:
                    evidence = item["evidence"]
                    candidate_id = str(item["candidate_id"])
                    representative = bool(
                        evidence is not None
                        and representatives.get(str(item.get("cluster_key"))) is item
                    )
                    rank = int(rank_by_id.get(candidate_id, 0))
                    reason = (
                        "DIVERSITY_CLUSTER_NON_REPRESENTATIVE"
                        if evidence is not None and not representative
                        else str(item["reason"])
                    )
                    fingerprint_rows.append(
                        {
                            "candidate_id": candidate_id,
                            "rank": rank,
                            "total_score": (
                                float(evidence["total_score"])
                                if evidence is not None
                                else None
                            ),
                            "component_scores": evidence or {},
                            "evidence_versions": item["versions"],
                            "cluster_key": str(item.get("cluster_key") or ""),
                            "cluster_representative": int(representative),
                            "selected": int(candidate_id == selected_id and representative),
                            "reason": reason,
                            "qualification_hash": item["qualification_hash"],
                            "ranking_snapshot_hash": item["ranking_snapshot_hash"],
                        }
                    )
                fingerprint_rows.sort(
                    key=lambda row: (
                        1 if int(row["rank"]) == 0 else 0,
                        int(row["rank"]),
                        (
                            -float(row["total_score"])
                            if row["total_score"] is not None
                            else float("inf")
                        ),
                        str(row["candidate_id"]),
                    )
                )
                run_id = "rank-" + hashlib.sha256(
                    json.dumps(
                        {
                            "formula_version": self.FORMULA_VERSION,
                            "ordered_rows": fingerprint_rows,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ).encode("utf-8")
                ).hexdigest()[:24]
                for item in stable:
                    evidence = item["evidence"]
                    candidate_id = str(item["candidate_id"])
                    representative = bool(
                        evidence is not None
                        and representatives.get(str(item.get("cluster_key"))) is item
                    )
                    selected = candidate_id == selected_id and representative
                    if evidence is not None and not representative:
                        reason = "DIVERSITY_CLUSTER_NON_REPRESENTATIVE"
                    else:
                        reason = str(item["reason"])
                    persisted.append(
                        {
                            "candidate_id": candidate_id,
                            "ranking_run_id": run_id,
                            "ranking_timestamp": timestamp.isoformat(),
                            "rank": int(rank_by_id.get(candidate_id, 0)),
                            "total_score": (
                                float(evidence["total_score"])
                                if evidence is not None
                                else None
                            ),
                            "component_scores_json": json.dumps(
                                evidence or {},
                                sort_keys=True,
                                allow_nan=False,
                            ),
                            "evidence_versions_json": json.dumps(
                                item["versions"],
                                sort_keys=True,
                                allow_nan=False,
                            ),
                            "cluster_key": str(item.get("cluster_key") or ""),
                            "cluster_representative": int(representative),
                            "selected": int(selected),
                            "reason": reason,
                            "qualification_hash": item["qualification_hash"],
                            "ranking_snapshot_hash": item["ranking_snapshot_hash"],
                        }
                    )
                previous = connection.execute(
                    "SELECT candidate_id,last_selected_candidate "
                    "FROM canary_selection WHERE singleton=1"
                ).fetchone()
                previous_id = (
                    str(
                        previous["candidate_id"]
                        or previous["last_selected_candidate"]
                        or ""
                    ).strip()
                    if previous is not None
                    else ""
                )
                historical_id = (
                    previous_id
                    if previous_id and previous_id != selected_id
                    else selected_id or previous_id or None
                )
                if selected_id:
                    selection_status = "CURRENT"
                    selection_valid = 1
                    invalidation_reason = None
                elif historical_id:
                    selection_status = "STALE"
                    selection_valid = 0
                    if previous_id in changed_ids:
                        invalidation_reason = "RANKING_EVIDENCE_CHANGED"
                    elif previous_id in invalidated_reasons:
                        invalidation_reason = invalidated_reasons[previous_id]
                    elif previous_id:
                        lifecycle = self.store.load_candidate_lifecycle(previous_id)
                        if (
                            not isinstance(lifecycle, Mapping)
                            or str(lifecycle.get("stage")) == "REJECTED"
                        ):
                            invalidation_reason = "LIFECYCLE_REJECTED"
                        else:
                            invalidation_reason = "REEVALUATION_REQUIRED"
                    else:
                        invalidation_reason = "REEVALUATION_REQUIRED"
                else:
                    selection_status = "NONE"
                    selection_valid = 0
                    invalidation_reason = None
                connection.execute("DELETE FROM canary_rankings")
                connection.executemany(
                    "INSERT INTO canary_rankings(candidate_id,ranking_run_id,ranking_timestamp,rank,total_score,"
                    "component_scores_json,evidence_versions_json,cluster_key,cluster_representative,selected,reason,"
                    "qualification_hash,ranking_snapshot_hash) "
                    "VALUES(:candidate_id,:ranking_run_id,:ranking_timestamp,:rank,:total_score,:component_scores_json,"
                    ":evidence_versions_json,:cluster_key,:cluster_representative,:selected,:reason,"
                    ":qualification_hash,:ranking_snapshot_hash)",
                    persisted,
                )
                winner = next((item for item in persisted if item["selected"]), None)
                connection.execute(
                    "INSERT INTO canary_selection(singleton,ranking_run_id,ranking_timestamp,candidate_id,rank,total_score,"
                    "component_scores_json,evidence_versions_json,reason,selected_at,qualification_hash,"
                    "ranking_snapshot_hash,selection_status,selection_valid,selection_invalidation_reason,"
                    "last_selected_candidate) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET ranking_run_id=excluded.ranking_run_id,"
                    "ranking_timestamp=excluded.ranking_timestamp,candidate_id=excluded.candidate_id,"
                    "rank=excluded.rank,total_score=excluded.total_score,"
                    "component_scores_json=excluded.component_scores_json,evidence_versions_json=excluded.evidence_versions_json,"
                    "reason=excluded.reason,selected_at=excluded.selected_at,"
                    "qualification_hash=excluded.qualification_hash,ranking_snapshot_hash=excluded.ranking_snapshot_hash,"
                    "selection_status=excluded.selection_status,selection_valid=excluded.selection_valid,"
                    "selection_invalidation_reason=excluded.selection_invalidation_reason,"
                    "last_selected_candidate=excluded.last_selected_candidate",
                    (
                        run_id,
                        timestamp.isoformat(),
                        winner["candidate_id"] if winner else None,
                        winner["rank"] if winner else None,
                        winner["total_score"] if winner else None,
                        winner["component_scores_json"] if winner else "{}",
                        winner["evidence_versions_json"] if winner else "{}",
                        "SELECTED_WINNER" if winner else str(invalidation_reason or "NO_ELIGIBLE_RANKABLE_CANDIDATE"),
                        timestamp.isoformat(),
                        winner["qualification_hash"] if winner else None,
                        winner["ranking_snapshot_hash"] if winner else None,
                        selection_status,
                        selection_valid,
                        invalidation_reason,
                        historical_id,
                    ),
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        with self.store._lock:
            self.service.bind_autonomous_selection(
                selected_id,
                publish_readiness=False,
            )
            public_status = self.service.publish_readiness_snapshot(
                reason="RANKING_EVALUATED"
            )
            # The committed ranking already validated selection hashes; avoid
            # re-running the expensive selection validation for this result.
            selection_record = self.service._selection_record()
            selected_snapshot = (
                selection_record
                if selected_id
                and isinstance(selection_record, Mapping)
                and str(selection_record.get("candidate_id") or "").strip()
                == selected_id
                else None
            )
            rankings_snapshot = self.rankings()
        return {
            "ranking_run_id": run_id,
            "evaluated_at": timestamp.isoformat(),
            "ranking_timestamp": timestamp.isoformat(),
            "winner_id": public_status.get("winner_id"),
            "selected_candidate": public_status.get("selected_candidate"),
            "last_selected_candidate": public_status.get("last_selected_candidate"),
            "selection_status": public_status.get("selection_status"),
            "selection_valid": public_status.get("selection_valid"),
            "selection_invalidation_reason": public_status.get(
                "selection_invalidation_reason"
            ),
            "selected": selected_snapshot,
            "rankings": rankings_snapshot,
            "eligibility_raw_count": public_status.get("eligibility_raw_count", 0),
            "eligible_count": public_status.get("eligible_count", 0),
            "rankable_raw_count": public_status.get("rankable_raw_count", 0),
            "rankable_count": public_status.get("rankable_count", 0),
            "holdout_used": False,
            "formula_version": self.FORMULA_VERSION,
        }

    def evaluate(self, now: datetime | None = None) -> dict[str, Any]:
        return self.evaluate_and_select(now)

    def rank(self, now: datetime | None = None) -> dict[str, Any]:
        return self.evaluate_and_select(now)

    MAX_RANKINGS = 10_000

    def rankings(self, *, limit: int = 100) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), self.MAX_RANKINGS))
        with self.store._lock:
            rows = self.store.connection.execute(
                "SELECT * FROM canary_rankings ORDER BY CASE WHEN rank=0 THEN 1 ELSE 0 END,rank,total_score DESC,candidate_id LIMIT ?",
                (bounded,),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                for key in ("component_scores_json", "evidence_versions_json"):
                    try:
                        value = json.loads(item.get(key) or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        value = {}
                    item[key.removesuffix("_json")] = value
                result.append(item)
            return result

    def current_selection(self) -> dict[str, Any] | None:
        # Keep the selection record and its validation on one store snapshot.
        # AxiomStore's lock is reentrant because both helpers may acquire it
        # while loading their constituent rows.
        with self.store._lock:
            selection = self.service._selection_record()
            if not isinstance(selection, Mapping):
                return None
            candidate_id = str(selection.get("candidate_id") or "").strip()
            lifecycle = (
                self.store.load_candidate_lifecycle(candidate_id)
                if candidate_id
                else None
            )
            if (
                not isinstance(lifecycle, Mapping)
                or str(lifecycle.get("stage") or "") not in self._STAGES
            ):
                return None
            payload = self.service._merged_lifecycle_payload(lifecycle)
            if not isinstance(payload, Mapping):
                return None
            if self._prediction_market(payload) not in {
                "prediction",
                "polymarket",
                "prediction_market",
            }:
                return None
            if _canonical_scope_gate_error(
                str(lifecycle.get("stage") or ""),
                payload,
            ) is not None:
                return None
            scope_resolution = _canary_current_scope_resolution(
                self.store,
                candidate_id,
                payload,
                now=ensure_utc(self.clock()),
            )
            if not scope_resolution.get("bound"):
                return None
            state = self.service._selection_validation(selection)
            return selection if state.get("selection_valid") else None

    def current_winner(self) -> dict[str, Any] | None:
        return self.current_selection()

    def eligible_count(self) -> int:
        return int(self.service.authoritative_status().get("eligible_count") or 0)


__all__ = ["CandidateCanaryRanker"]
