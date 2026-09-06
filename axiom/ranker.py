"""Deterministic, persisted prediction-candidate canary selection."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping

from .canary import CanaryService
from .domain import ensure_utc, utc_now
from .storage import AxiomStore


class CandidateCanaryRanker:
    """Continuously evaluate and rank frozen prediction candidates.

    Ranking reads only immutable validation evidence and the optional paper
    forward evidence already persisted on the candidate.  The locked holdout
    partition is intentionally not read by this class.
    """

    FORMULA_VERSION = "validation-weighted-v1"
    WEIGHTS = {
        "expectancy": 0.20,
        "confidence_lower_bound": 0.15,
        "robustness": 0.15,
        "sample": 0.10,
        "execution_feasibility": 0.10,
        "data_quality": 0.10,
        "calibration": 0.10,
        "drawdown": 0.05,
        "liquidity": 0.05,
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
        self.service = service or CanaryService(store)
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
    ) -> tuple[dict[str, Any] | None, str | None, dict[str, Any]]:
        versions = cls._versions(payload, str(frozen_hash or ""))
        if not frozen_hash:
            return None, "FROZEN_HASH_MISSING", versions
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
            "data_quality": cls._quality(
                payload, "validation_data_quality", "data_quality", "quality", "data_quality_passed"
            ),
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            return None, "RANKING_EVIDENCE_MISSING:" + ",".join(missing), versions
        if required["sample_count"] == 0 or required["trade_count"] == 0:
            return None, "RANKING_EVIDENCE_EMPTY_SAMPLE", versions
        plan = payload.get("experiment_plan") if isinstance(payload.get("experiment_plan"), Mapping) else {}
        min_samples = cls._number(plan.get("min_independent_samples", plan.get("min_samples", 30))) or 30.0
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
            "data_quality": cls._clamp(float(required["data_quality"])),
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
        evidence = {
            "raw": {
                "validation_expectancy": required["expectancy"],
                "validation_confidence_lower_bound": required["confidence_lower_bound"],
                "validation_stability": required["robustness"],
                "validation_calibration": required["calibration"],
                "validation_sample_count": required["sample_count"],
                "validation_trade_count": required["trade_count"],
                "validation_execution_quality": required["execution_feasibility"],
                "validation_data_quality": required["data_quality"],
                "validation_max_drawdown": drawdown,
                "validation_liquidity": liquidity,
                "forward_expectancy": forward_expectancy,
            },
            "components": components,
            "weights": weights,
            "total_score": score,
            "versions": versions,
        }
        return evidence, None, versions

    def _candidate_records(self) -> list[Mapping[str, Any]]:
        records = self.store.load_candidate_lifecycle(limit=10000)
        return [item for item in records if isinstance(item, Mapping)] if isinstance(records, list) else []

    def evaluate_and_select(self, now: datetime | None = None) -> dict[str, Any]:
        timestamp = ensure_utc(now or self.clock())
        candidates: list[dict[str, Any]] = []
        for record in self._candidate_records():
            candidate_id = str(record.get("candidate_id") or "").strip()
            payload = record.get("payload")
            if not candidate_id or not isinstance(payload, Mapping) or str(record.get("stage")) not in self._STAGES:
                continue
            if self._prediction_market(payload) not in {"prediction", "polymarket", "prediction_market"}:
                self.service.invalidate_eligibility(candidate_id, "PREDICTION_MARKET_ONLY")
                continue
            validation = self.service.validate_eligibility(candidate_id)
            if not validation.get("eligible"):
                self.service.invalidate_eligibility(candidate_id, str(validation.get("reason_code") or "GATES_INCOMPLETE"))
                continue
            try:
                self.service.mark_eligible(candidate_id)
            except Exception:
                self.service.invalidate_eligibility(candidate_id, "ELIGIBILITY_BINDING_PERSIST_FAILED")
                continue
            evidence, reason, versions = self._rank_evidence(payload, validation.get("frozen_hash"))
            if evidence is None:
                candidates.append({
                    "candidate_id": candidate_id,
                    "payload": dict(payload),
                    "evidence": None,
                    "reason": reason or "RANKING_EVIDENCE_MISSING",
                    "versions": versions,
                })
                continue
            cluster_key = self._cluster_key(candidate_id, payload, versions)
            candidates.append({
                "candidate_id": candidate_id,
                "payload": dict(payload),
                "evidence": evidence,
                "reason": "",
                "versions": versions,
                "cluster_key": cluster_key,
            })

        ranked = [item for item in candidates if item["evidence"] is not None]
        ranked.sort(
            key=lambda item: (
                -float(item["evidence"]["total_score"]),
                -float(item["evidence"]["raw"]["validation_confidence_lower_bound"]),
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
                -float(item["evidence"]["raw"]["validation_confidence_lower_bound"]),
                -float(item["evidence"]["raw"]["validation_expectancy"]),
                str(item["candidate_id"]),
            ),
        )
        rank_by_id = {str(item["candidate_id"]): index for index, item in enumerate(selected_representatives, start=1)}
        selected_id = str(selected_representatives[0]["candidate_id"]) if selected_representatives else None
        evidence_seed = [
            (str(item["candidate_id"]), item["versions"], float(item["evidence"]["total_score"]))
            for item in selected_representatives
        ]
        run_id = "rank-" + hashlib.sha256(
            json.dumps(evidence_seed, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        ).hexdigest()[:24]
        persisted = []
        for item in candidates:
            evidence = item["evidence"]
            candidate_id = str(item["candidate_id"])
            representative = bool(evidence is not None and representatives.get(str(item.get("cluster_key"))) is item)
            selected = candidate_id == selected_id and representative
            if evidence is not None and not representative:
                reason = "DIVERSITY_CLUSTER_NON_REPRESENTATIVE"
            else:
                reason = str(item["reason"])
            persisted.append({
                "candidate_id": candidate_id,
                "ranking_run_id": run_id,
                "ranking_timestamp": timestamp.isoformat(),
                "rank": int(rank_by_id.get(candidate_id, 0)),
                "total_score": float(evidence["total_score"]) if evidence is not None else None,
                "component_scores_json": json.dumps(evidence or {}, sort_keys=True, allow_nan=False),
                "evidence_versions_json": json.dumps(item["versions"], sort_keys=True, allow_nan=False),
                "cluster_key": str(item.get("cluster_key") or ""),
                "cluster_representative": int(representative),
                "selected": int(selected),
                "reason": reason,
            })
        with self.store._lock:
            with self.store.connection:
                self.store.connection.execute("DELETE FROM canary_rankings")
                self.store.connection.executemany(
                    "INSERT INTO canary_rankings(candidate_id,ranking_run_id,ranking_timestamp,rank,total_score,"
                    "component_scores_json,evidence_versions_json,cluster_key,cluster_representative,selected,reason) "
                    "VALUES(:candidate_id,:ranking_run_id,:ranking_timestamp,:rank,:total_score,:component_scores_json,"
                    ":evidence_versions_json,:cluster_key,:cluster_representative,:selected,:reason)",
                    persisted,
                )
                winner = next((item for item in persisted if item["selected"]), None)
                self.store.connection.execute(
                    "INSERT INTO canary_selection(singleton,ranking_run_id,candidate_id,rank,total_score,"
                    "component_scores_json,evidence_versions_json,reason,selected_at) VALUES(1,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET ranking_run_id=excluded.ranking_run_id,"
                    "candidate_id=excluded.candidate_id,rank=excluded.rank,total_score=excluded.total_score,"
                    "component_scores_json=excluded.component_scores_json,evidence_versions_json=excluded.evidence_versions_json,"
                    "reason=excluded.reason,selected_at=excluded.selected_at",
                    (
                        run_id,
                        winner["candidate_id"] if winner else None,
                        winner["rank"] if winner else None,
                        winner["total_score"] if winner else None,
                        winner["component_scores_json"] if winner else "{}",
                        winner["evidence_versions_json"] if winner else "{}",
                        "SELECTED_WINNER" if winner else "NO_ELIGIBLE_RANKABLE_CANDIDATE",
                        timestamp.isoformat(),
                    ),
                )
        self.service.bind_autonomous_selection(selected_id)
        return {
            "ranking_run_id": run_id,
            "evaluated_at": timestamp.isoformat(),
            "selected_candidate": selected_id,
            "selected": self.current_selection(),
            "rankings": self.rankings(),
            "eligible_count": self.eligible_count(),
            "rankable_count": len(ranked),
            "holdout_used": False,
            "formula_version": self.FORMULA_VERSION,
        }

    def evaluate(self, now: datetime | None = None) -> dict[str, Any]:
        return self.evaluate_and_select(now)

    def rank(self, now: datetime | None = None) -> dict[str, Any]:
        return self.evaluate_and_select(now)

    def rankings(self, *, limit: int = 100) -> list[dict[str, Any]]:
        bounded = max(1, min(int(limit), 1000))
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
        return self.service._selection_record()

    def current_winner(self) -> dict[str, Any] | None:
        return self.current_selection()

    def eligible_count(self) -> int:
        row = self.store.connection.execute("SELECT COUNT(*) AS n FROM canary_eligibility").fetchone()
        return int(row["n"] if row is not None else 0)


__all__ = ["CandidateCanaryRanker"]
