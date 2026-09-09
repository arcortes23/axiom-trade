from __future__ import annotations

import unittest

from axiom.autonomous import AutonomousResearchError, AutonomousResearchProcessor
from axiom.storage import AxiomStore

from axiom.experiment_plan import (
    ExperimentPlan,
    ExperimentPlanError,
    MarketScopeMode,
    historical_market_ids,
    normalize_market_scope,
)


class MarketScopeContractTests(unittest.TestCase):
    @staticmethod
    def _base(**extra: object) -> dict[str, object]:
        value: dict[str, object] = {
            "hypothesis_id": "scope-contract",
            "market_type": "prediction",
            "template": "probability_mispricing",
            "dataset_version": "dataset-v1",
            "parameters": {"threshold": [0.05]},
            "paper_only": True,
        }
        value.update(extra)
        return value

    def test_legacy_properties_are_views_of_canonical_scope_and_round_trip(self) -> None:
        plan = ExperimentPlan.from_mapping(
            self._base(
                filters={"category": "Politics", "min_liquidity": 10},
                regime_restrictions={"allowed_regimes": ["calm"]},
                target={"instrument": "POLYMARKET", "market_ids": ["m2", "m1"]},
            )
        )
        self.assertEqual(plan.market_scope.mode, MarketScopeMode.EXACT_MARKETS.value)
        self.assertEqual(plan.target_markets, ("m2", "m1"))
        self.assertEqual(plan.target["market_ids"], ["m2", "m1"])
        self.assertEqual(plan.filters["category"], "politics")
        self.assertEqual(plan.regime_restrictions["regimes"], ("calm",))
        round_trip = ExperimentPlan.from_mapping(plan.as_dict())
        self.assertEqual(round_trip.plan_hash, plan.plan_hash)
        self.assertEqual(round_trip.market_scope_hash, plan.market_scope_hash)

    def test_exact_ids_and_filters_are_an_intersection(self) -> None:
        scope = normalize_market_scope(
            target={"market_ids": ["b", "a"]},
            filters={"category": "politics", "min_liquidity": 10},
        )
        self.assertEqual(scope.mode, MarketScopeMode.EXACT_MARKETS.value)
        self.assertEqual(scope.market_ids, ("b", "a"))
        self.assertEqual(scope.filters["category"], "politics")
        self.assertEqual(scope.filters["min_liquidity"], 10.0)

    def test_polymarket_history_allows_unidentified_rows_but_rejects_mismatches(self) -> None:
        plan = ExperimentPlan.from_mapping(
            self._base(
                target={"instrument": "POLYMARKET"},
                dataset_selector={
                    "dataset_id": "Polymarket-historical",
                    "dataset_version": "dataset-v1",
                    "source_type": "HISTORICAL",
                },
            )
        )
        rows = [
            {"timestamp": "2025-01-01T00:00:00Z", "yes_mid": 0.40},
            {"market_id": "market-symbol", "symbol": "POLYMARKET", "yes_mid": 0.45},
            {"market_id": "market-instrument", "instrument": "polymarket", "yes_mid": 0.50},
            {"market_id": "wrong-instrument", "instrument": "OTHER", "yes_mid": 0.55},
            {"market_id": "wrong-symbol", "symbol": "OTHER", "yes_mid": 0.60},
        ]
        with AxiomStore(":memory:") as store:
            selected = AutonomousResearchProcessor(store)._apply_plan_filters(plan, rows)
        self.assertEqual([row.get("market_id") for row in selected], [None, "market-symbol", "market-instrument"])

    def test_historical_scope_filter_does_not_replace_dataset_attestation_gate(self) -> None:
        plan = ExperimentPlan.from_mapping(
            self._base(
                target={"instrument": "POLYMARKET"},
                dataset_selector={
                    "dataset_id": "Polymarket-historical",
                    "dataset_version": "dataset-v1",
                    "source_type": "HISTORICAL",
                },
            )
        )

        class MissingAttestationStore(AxiomStore):
            def load_dataset_integrity_attestation(self, dataset_id: str, dataset_version: str) -> None:
                return None

            def verify_dataset_integrity_attestation(self, dataset_id: str, dataset_version: str) -> None:
                return None

        with MissingAttestationStore(":memory:") as store:
            processor = AutonomousResearchProcessor(store)
            with self.assertRaisesRegex(AutonomousResearchError, "DATASET_ATTESTATION_MISSING"):
                processor._dataset_attestation(plan)

    def test_malformed_and_conflicting_authority_fails_closed(self) -> None:
        with self.assertRaisesRegex(ExperimentPlanError, "MALFORMED_MARKET_SCOPE"):
            normalize_market_scope({"mode": "EXACT_MARKETS", "market_ids": []})
        with self.assertRaisesRegex(ExperimentPlanError, "CONFLICTING_MARKET_SCOPE"):
            ExperimentPlan.from_mapping(
                self._base(
                    market_scope={"mode": "RULE_BASED_MARKETS", "categories": ["politics"]},
                    target={"market_ids": ["m1"]},
                )
            )
        with self.assertRaisesRegex(ExperimentPlanError, "CONFLICTING_MARKET_SCOPE"):
            ExperimentPlan.from_mapping(
                self._base(
                    market_scope={"mode": "RULE_BASED_MARKETS", "categories": ["politics"]},
                    market_scope_hash="sha256:not-the-policy",
                )
            )

    def test_scope_hash_is_deterministic_for_unordered_inputs(self) -> None:
        first = normalize_market_scope(
            market_ids={"m2", "m1"}, categories={"Politics", "Sports"}, filters={"category": {"sports", "politics"}}
        )
        second = normalize_market_scope(
            market_ids=["m1", "m2"], categories=["sports", "politics"], filters={"category": ["politics", "sports"]}
        )
        self.assertEqual(first.policy_hash, second.policy_hash)

    def test_top_level_proposal_alias_is_preserved_in_embedded_plan(self) -> None:
        proposal = {
            "proposal_id": "top-level-scope",
            "statement": "A bounded test is possible.",
            "source": "fixture",
            "tests": ["chronological backtest"],
            "dataset_version": "dataset-v1",
            "paper_only": True,
            "market_scope": {"mode": "RULE_BASED_MARKETS", "instrument": "POLYMARKET", "categories": ["politics"]},
            "experiment_plan": self._base(),
        }
        plan = ExperimentPlan.from_proposal(proposal)
        self.assertEqual(plan.market_scope.mode, MarketScopeMode.RULE_BASED_MARKETS.value)
        self.assertEqual(plan.market_scope.instrument, "POLYMARKET")
        self.assertEqual(plan.market_scope.categories, ("politics",))

    def test_historical_constituents_do_not_become_current_market_authority(self) -> None:
        plan = ExperimentPlan.from_mapping(
            self._base(
                dataset_version="aggregate-v1",
                dataset_selector={
                    "dataset_version": "aggregate-v1",
                    "source_type": "historical",
                    "constituent_market_ids": ["historical-1", "historical-2"],
                }
            )
        )
        self.assertEqual(plan.dataset_source_type, "HISTORICAL")
        self.assertEqual(historical_market_ids(plan.dataset_selector), ("historical-1", "historical-2"))
        self.assertEqual(plan.target_markets, ())
        self.assertEqual(plan.market_scope.mode, MarketScopeMode.RESEARCH_ONLY.value)

    def test_frozen_payload_material_contains_scope_and_selector_bindings(self) -> None:
        plan = ExperimentPlan.from_mapping(
            self._base(
                market_scope={"mode": "RULE_BASED_MARKETS", "categories": ["politics"]},
                dataset_version="aggregate-v1",
                dataset_selector={
                    "dataset_id": "Polymarket-historical",
                    "dataset_version": "aggregate-v1",
                    "source_type": "HISTORICAL",
                },
            )
        )
        record = plan.as_dict()
        self.assertEqual(record["market_scope_hash"], plan.market_scope_hash)
        self.assertEqual(record["market_scope_version"], plan.market_scope_version)
        self.assertEqual(record["dataset_selector"]["source_type"], "HISTORICAL")
        self.assertEqual(record["market_scope"]["mode"], MarketScopeMode.RULE_BASED_MARKETS.value)


if __name__ == "__main__":
    unittest.main()
