from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import unittest

from axiom.director import validate_hermes_proposal
from axiom.storage import AxiomStore


_PROMPT = Path(__file__).parents[1] / "hermes" / "axiom_research_director_prompt.md"
_DATASET_ID = "Polymarket-historical"
_DATASET_VERSION = "sha256:polymarket-historical-v1"


def _prompt_example() -> dict[str, object]:
    text = _PROMPT.read_text(encoding="utf-8")
    blocks = re.findall(r"```json\s*\n(.*?)\n\s*```", text, flags=re.DOTALL)
    if len(blocks) != 1:
        raise AssertionError(f"expected exactly one JSON schema example, found {len(blocks)}")
    value = json.loads(blocks[0])
    if not isinstance(value, dict):
        raise AssertionError("director example must be a JSON object")
    return value


class HermesMarketScopePromptTests(unittest.TestCase):
    def test_rule_based_prompt_example_passes_real_proposal_validation(self) -> None:
        proposal = _prompt_example()
        plan = proposal.get("experiment_plan")
        self.assertIsInstance(plan, dict)
        assert isinstance(plan, dict)
        scope = plan.get("market_scope")
        self.assertIsInstance(scope, dict)
        assert isinstance(scope, dict)
        self.assertEqual(scope["schema_version"], "1")
        self.assertEqual(scope["mode"], "RULE_BASED_MARKETS")
        self.assertEqual(scope["instrument"], "POLYMARKET")
        self.assertEqual(scope["categories"], ["politics"])
        self.assertEqual(scope["market_ids"], [])
        self.assertNotIn("filters", plan)
        self.assertNotIn("target", plan)
        self.assertNotIn("market_scope_hash", plan)
        self.assertNotIn("market_scope_version", plan)
        self.assertEqual(
            set(scope["filters"]),
            {
                "entry_price",
                "minimum_hours_to_resolution",
                "maximum_hours_to_resolution",
                "min_liquidity",
                "max_spread",
            },
        )
        self.assertEqual(scope["regime_restrictions"], {})
        self.assertEqual(scope["provenance"], "canonical")

        selector = plan.get("dataset_selector")
        self.assertIsInstance(selector, dict)
        assert isinstance(selector, dict)
        self.assertEqual(selector["dataset_id"], _DATASET_ID)
        self.assertEqual(selector["dataset_version"], _DATASET_VERSION)
        self.assertNotEqual(selector, scope)
        self.assertNotIn("market_versions", selector)

        # The catalog is the exact historical binding that the real validator
        # checks; no Hermes execution, worker, or scheduler is involved.
        with AxiomStore(":memory:") as store:
            store.save_dataset_catalog(
                _DATASET_ID,
                _DATASET_VERSION,
                provider="fixture-provider",
                instrument="POLYMARKET",
                market_type="prediction",
                timeframe="event",
                start_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                end_timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc),
                row_count=100,
                completeness=1.0,
                missing_ranges=(),
                quality="PRICE_PROXY",
                source_type="HISTORICAL",
                snapshot_id=f"{_DATASET_ID}:{_DATASET_VERSION}",
                metadata={},
            )
            validation = validate_hermes_proposal(proposal, store=store)

        self.assertTrue(validation.accepted, validation.reasons)
        assert validation.normalized is not None
        normalized_plan = validation.normalized["experiment_plan"]
        self.assertEqual(normalized_plan["market_scope"]["mode"], "RULE_BASED_MARKETS")
        self.assertEqual(normalized_plan["dataset_selector"]["dataset_id"], _DATASET_ID)
        self.assertEqual(normalized_plan["dataset_selector"]["dataset_version"], _DATASET_VERSION)
        self.assertNotIn("market_scope_hash", scope)
        self.assertNotIn("market_scope_version", scope)


if __name__ == "__main__":
    unittest.main()
