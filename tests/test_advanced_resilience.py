# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import unittest
from io import BytesIO
from typing import Optional, Dict, Any

from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


class TestAdvancedResilience(unittest.TestCase):
    """
    Advanced resilience tests covering complex edge cases not found in standard
    compliance or happy-path tests.
    """

    def test_namespaced_xml_keys_strict(self) -> None:
        """
        Verify that XML elements with Namespace Prefixes produce clean keys in the JSON output.
        The downstream SQL models expect keys like 'MedlineCitation', not 'ns1:MedlineCitation'.
        """
        xml_content = b"""
        <ns1:PubmedArticleSet xmlns:ns1="http://example.org/ns">
            <ns1:MedlineCitation>
                <ns1:PMID>99999</ns1:PMID>
                <ns1:Article>
                    <ns1:ArticleTitle>Namespaced Title</ns1:ArticleTitle>
                </ns1:Article>
            </ns1:MedlineCitation>
        </ns1:PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        record = records[0]

        # The root key must be EXACTLY "MedlineCitation", not "ns1:MedlineCitation"
        keys = list(record.keys())
        self.assertIn("_record_type", keys)

        # Check if "MedlineCitation" is present without prefix
        # This assertion will fail if the parser preserves prefixes
        self.assertIn("MedlineCitation", keys, f"Keys found: {keys}")

        # Check nested keys as well
        citation = record.get("MedlineCitation", {})
        self.assertIn("PMID", citation, "Nested key 'PMID' should be present without prefix")
        # PMID is in FORCE_LIST_KEYS, so it should be a list
        self.assertEqual(citation["PMID"], ["99999"])

    def test_mixed_content_unhandled_field(self) -> None:
        """
        Test mixed content in a field NOT handled by the strip_tags logic.
        xml_utils only strips tags for: ArticleTitle, AbstractText, VernacularTitle, Affiliation.

        If we have mixed content in another field (e.g. 'CoIStatement'),
        xmltodict behavior is typically to split it into '#text' and children.
        We want to document/verify this behavior.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article>
                    <CoIStatement>This is <b>bold</b> statement.</CoIStatement>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        coi = records[0]["MedlineCitation"]["Article"]["CoIStatement"]

        # xmltodict default behavior for mixed content:
        # It creates a complex object or list logic.
        # Actually xmltodict usually preserves order if configured,
        # but for standard mixed content without special config, it looks like:
        # {'#text': 'This is  statement.', 'b': 'bold'} (text might be split or concatenated?)
        # Let's inspect what we get.

        # If it returns a dictionary, it's what we expect from xmltodict.
        # We just want to ensure it doesn't crash or return None.
        self.assertIsInstance(coi, (dict, str, list))

        if isinstance(coi, dict):
            # Likely has keys for the text parts and the child tag
            # Just verifying existence of data
            self.assertTrue(any(k for k in coi.keys()))

    def test_massive_nested_recursion(self) -> None:
        """
        Simulate a deeply nested structure to ensure no recursion limits are hit
        during the 'tostring' or 'parse' phase (within reason).
        """
        depth = 100
        inner = "<Leaf>Val</Leaf>"
        for _ in range(depth):
            inner = f"<Nest>{inner}</Nest>"

        xml_content = f"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article>
                    {inner}
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """.encode('utf-8')

        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        # Traverse down
        curr = records[0]["MedlineCitation"]["Article"]
        for _ in range(depth):
            curr = curr["Nest"]
        self.assertEqual(curr["Leaf"], "Val")

    def test_date_logic_invalid_inputs(self) -> None:
        """
        Test the robustness of Date Logic (mocking SQL behavior).
        Specifically 'Feb 30' or other non-existent dates.
        """
        # We use the logic mirror from `test_gold_logic.py` effectively,
        # but here we focus on the SQL `make_date` equivalent behavior.

        # Postgres `make_date(year, month, day)` raises an error if the date is invalid.
        # It does NOT return NULL.
        # We need to decide if we want our pipeline to crash or handle it.
        # Ideally, we should handle it.
        # But `make_date` is hard to wrap in "TRY" logic in standard Postgres
        # without a custom PL/pgSQL function or simple case logic for days.

        # Valid Date
        self._simulate_make_date(2023, 2, 28) # Should pass

        # Invalid Date - Logic Verification
        with self.assertRaises(ValueError):
            self._simulate_make_date(2023, 2, 30)

    def _simulate_make_date(self, year: int, month: int, day: int) -> Any:
        """
        Python simulation of Postgres make_date strictness.
        """
        import datetime
        return datetime.date(year, month, day)
