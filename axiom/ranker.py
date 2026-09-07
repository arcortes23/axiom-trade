"""Deterministic, persisted prediction-candidate canary selection."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping

from .canary import (
    CanaryService,
    _canary_qualification_hash,
    _canary_qualification_projection,
    _canary_ranking_snapshot_hash,
)
from .domain import ensure_utc, utc_now

from .storage import AxiomStore
from .lifecycle import CandidateLifecycleManager, CandidateStage
from .data_quality import evaluate_prediction_data_quality, persisted_quality_fields


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
    _STAGES = frozenset({"FROZEN", "PAPER_FORWARD"})
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
        plan = payload.get("experiment_plan")
        strategy = payload.get("strategy")
        forward = payload.get("forward_config")
        market = payload.get("market")
        for source in (payload, plan, strategy, forward, market):
            if isinstance(source, Mapping):
                value = source.get("market_type", source.get("type"))
                if value is not None:
                    return str(value).strip().lower()
        value = payload.get("market_type")
        return str(value).strip().lower() if value is not None else None

    @classmethod
    def _sample_count(cls, payload: Mapping[str, Any]) -> int | None:
        value = cls._value(payload, "validation_sample_count", "sample_count")
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
        value = cls._value(payload, "validation_trade_count", "validation_trades", "trade_count")
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
            "strategy_hash": versions.get("strategy_hash"),
            "model_hash": versions.get("model_hash"),
            "config_hash": versions.get("config_hash"),
        }
        encoded = json.dumps(material, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return "cluster-" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:20]

    @classmethod
    def _versions(cls, payload: Mapping[str, Any], frozen_hash: str) -> dict[str, Any]:
        plan = payload.get("experiment_plan") if isinstance(payload.get("experiment_plan"), Mapping) else {}
        return {
            "frozen_hash": frozen_hash,
            "strategy_hash": str(payload.get("strategy_hash") or ""),
            "model_hash": str(payload.get("model_hash") or ""),
            "config_hash": str(payload.get("config_hash") or ""),
            "dataset_id": str(payload.get("dataset_id") or plan.get("dataset_id") or ""),
            "dataset_version": str(payload.get("dataset_version") or plan.get("dataset_version") or ""),
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
            payload, "validation_data_quality", "data_quality", "quality", "data_quality_passed"
        )
        if isinstance(quality, Mapping) and quality.get("applicable"):
            integrity_score = (
                1.0 if quality.get("historical_data_integrity_passed") else None
            )
            fidelity_score = cls._number(quality.get("historical_execution_fidelity_score"))
        else:
            integrity_score = legacy_quality
            fidelity_score = cls._number(payload.get("execution_fidelity_score", legacy_quality))
        required = {
            "expectancy": cls._number(cls._value(payload, "validation_expectancy", "expectancy")),
            "confidence_lower_bound": cls._number(
                cls._value(payload, "validation_confidence_lower_bound", "confidence_lower_bound")
            ),
            "robustness": cls._number(cls._value(payload, "validation_stability", "stability")),
            "calibration": cls._number(cls._value(payload, "validation_calibration", "calibration")),
            "sample_count": cls._sample_count(payload),
            "trade_count": cls._trade_count(payload),
            "execution_feasibility": cls._quality(
                payload, "validation_execution_quality", "execution_quality"
            ),
            "historical_data_integrity": integrity_score,
            "execution_fidelity_score": fidelity_score,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            return None, "RANKING_EVIDENCE_MISSING:" + ",".join(missing), versions
        plan = payload.get("experiment_plan") if isinstance(payload.get("experiment_plan"), Mapping) else {}
        min_samples = cls._number(plan.get("min_independent_samples", plan.get("min_samples", 30))) or 30.0
        min_trades = cls._number(plan.get("min_trades", 0)) or 0.0
        if required["sample_count"] < min_samples or required["trade_count"] < min_trades:
            return None, "RANKING_EVIDENCE_BELOW_MINIMUM_SAMPLE", versions
        forward_evidence = payload.get("forward_evidence")
        forward_expectancy = cls._number(payload.get("forward_expectancy"))
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
        drawdown = cls._number(cls._value(payload, "validation_max_drawdown", "max_drawdown"))
        if drawdown is not None:
            optional["drawdown"] = cls._clamp(1.0 - drawdown)
        liquidity = cls._number(cls._value(payload, "validation_liquidity", "liquidity"))
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
    def _persist_quality_projection(
        self,
        record: Mapping[str, Any],
        payload: Mapping[str, Any],
        quality: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        if not quality.get("applicable"):
            return record, payload
        evidence = persisted_quality_fields(quality)
        if all(payload.get(key) == value for key, value in evidence.items()):
            return record, payload
        stage = str(record.get("stage") or "")
        if stage not in self._STAGES:
            return record, payload
        try:
            lifecycle = CandidateLifecycleManager(self.store)
            lifecycle.record_evidence(
                str(record.get("candidate_id") or ""),
                evidence,
                expected_stage=stage,
                reason="data-quality policy re-evaluation",
            )
        except (KeyError, RuntimeError, ValueError):
            return record, payload
        refreshed = self.store.load_candidate_lifecycle(str(record.get("candidate_id") or ""))
        if not isinstance(refreshed, Mapping) or not isinstance(refreshed.get("payload"), Mapping):
            return record, payload
        return refreshed, refreshed["payload"]


    def _snapshot_hashes(
        self,
        record: Mapping[str, Any],
        payload: Mapping[str, Any],
        quality: Mapping[str, Any],
        frozen_hash: str,
    ) -> tuple[dict[str, Any], str, str] | None:
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

    def evaluate_and_select(self, now: datetime | None = None) -> dict[str, Any]:
        timestamp = ensure_utc(now or self.clock())
        candidates: list[dict[str, Any]] = []
        invalidated_reasons: dict[str, str] = {}
        for original_record in self._candidate_records():
            candidate_id = str(original_record.get("candidate_id") or "").strip()
            if not candidate_id:
                continue
            stage = str(original_record.get("stage") or "")
            payload = original_record.get("payload")
            if stage not in self._STAGES or not isinstance(payload, Mapping):
                invalidated_reasons[candidate_id] = (
                    "LIFECYCLE_REJECTED"
                    if stage == "REJECTED"
                    else "ELIGIBILITY_INVALID"
                )
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                )
                continue
            quality = evaluate_prediction_data_quality(self.store, payload)
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
                )
                continue
            validation = self.service.validate_eligibility(
                candidate_id,
                _record=record,
            )
            if not validation.get("eligible"):
                binding_reason = (
                    validation.get("binding", {}).get("reason_code")
                    if isinstance(validation.get("binding"), Mapping)
                    else None
                )
                invalidated_reasons[candidate_id] = (
                    str(binding_reason)
                    if binding_reason in {
                        "QUALIFICATION_CHANGED",
                        "ELIGIBILITY_MISSING",
                        "ELIGIBILITY_INVALID",
                    }
                    else "ELIGIBILITY_INVALID"
                )
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                )
                continue
            try:
                self.service.mark_eligible(candidate_id)
            except Exception:
                invalidated_reasons[candidate_id] = "ELIGIBILITY_INVALID"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                )
                continue
            record = self.store.load_candidate_lifecycle(candidate_id)
            payload = (
                self.service._merged_lifecycle_payload(record)
                if isinstance(record, Mapping)
                else None
            )
            if not isinstance(record, Mapping) or not isinstance(payload, Mapping):
                invalidated_reasons[candidate_id] = "ELIGIBILITY_INVALID"
                self.service.invalidate_eligibility(
                    candidate_id,
                    invalidated_reasons[candidate_id],
                )
                continue
            quality = evaluate_prediction_data_quality(self.store, payload)
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
                )
                continue
            _, qualification_hash, ranking_snapshot_hash = hashes
            evidence, reason, versions = self._rank_evidence(
                payload,
                frozen_hash,
                quality=quality,
            )
            versions.update(
                {
                    "qualification_hash": qualification_hash,
                    "ranking_snapshot_hash": ranking_snapshot_hash,
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
            }
            if evidence is not None:
                item["reason"] = ""
                item["cluster_key"] = self._cluster_key(
                    candidate_id,
                    payload,
                    versions,
                )
            candidates.append(item)

        changed_ids: set[str] = set()
        stable: list[dict[str, Any]] = []
        with self.store._lock:
            connection = self.store.connection
            if connection.in_transaction:
                raise RuntimeError("ranking transaction already active")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for item in candidates:
                    candidate_id = str(item["candidate_id"])
                    current = self.store.load_candidate_lifecycle(candidate_id)
                    if not isinstance(current, Mapping):
                        changed_ids.add(candidate_id)
                        continue
                    current_payload = self.service._merged_lifecycle_payload(current)
                    if not isinstance(current_payload, Mapping):
                        changed_ids.add(candidate_id)
                        continue
                    current_quality = evaluate_prediction_data_quality(
                        self.store,
                        current_payload,
                    )
                    current_validation = self.service.validate_eligibility(
                        candidate_id,
                        _record=current,
                    )
                    current_frozen_hash = str(
                        current_validation.get("frozen_hash") or ""
                    )
                    current_hashes = self._snapshot_hashes(
                        current,
                        current_payload,
                        current_quality,
                        current_frozen_hash,
                    )
                    if (
                        not current_validation.get("eligible")
                        or current_hashes is None
                        or current_hashes[1] != item["qualification_hash"]
                        or current_hashes[2] != item["ranking_snapshot_hash"]
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
                evidence_seed = [
                    (
                        str(item["candidate_id"]),
                        item["versions"],
                        float(item["evidence"]["total_score"]),
                    )
                    for item in selected_representatives
                ]
                run_id = "rank-" + hashlib.sha256(
                    json.dumps(
                        evidence_seed,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ).encode("utf-8")
                ).hexdigest()[:24]
                persisted: list[dict[str, Any]] = []
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
            self.service.bind_autonomous_selection(selected_id)
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

    def rankings(self, *, limit: int = 100) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), 1000))
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
            state = self.service._selection_validation(selection)
            return selection if state.get("selection_valid") else None

    def current_winner(self) -> dict[str, Any] | None:
        return self.current_selection()

    def eligible_count(self) -> int:
        return int(self.service.authoritative_status().get("eligible_count") or 0)


__all__ = ["CandidateCanaryRanker"]
