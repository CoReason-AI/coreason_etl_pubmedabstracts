# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import re
import unittest
from typing import Any, Dict, List, Optional


class TestGoldLogic(unittest.TestCase):
    """
    Tests to verify the business logic used in Gold Layer SQL models.
    We reimplement the SQL logic in Python to verify it handles edge cases correctly.
    """

    def _extract_year_sql_logic(self, pub_year_str: Optional[str], medline_date_str: Optional[str]) -> Optional[int]:
        r"""
        Mimics the logic in `stg_pubmed_baseline.sql` and `gold_pubmed_knowledge.sql`.

        Logic:
        1. COALESCE(Year, substring(MedlineDate from '\d{4}')) as temp_year
        2. In Gold: case when temp_year ~ '^\d+$' then temp_year::int else null end
        """
        # Step 1: Coalesce
        temp_year = pub_year_str
        if not temp_year and medline_date_str:
            # Postgres substring(str from '\d{4}') returns the first match
            match = re.search(r"\d{4}", medline_date_str)
            if match:
                temp_year = match.group(0)

        # Step 2: Safe Cast (Gold Layer)
        if temp_year and re.match(r"^\d+$", temp_year):
            return int(temp_year)
        return None

    def test_publication_year_logic(self) -> None:
        # Case 1: Standard Year
        self.assertEqual(self._extract_year_sql_logic("2023", None), 2023)

        # Case 2: MedlineDate fallback
        self.assertEqual(self._extract_year_sql_logic(None, "1998 Dec-1999 Jan"), 1998)

        # Case 3: MedlineDate with text
        self.assertEqual(self._extract_year_sql_logic(None, "Spring 2000"), 2000)

        # Case 4: Invalid Year (e.g. "Unknown") -> Should return None
        # Because regex \d{4} won't match, or if extracted, strict check might fail?
        # Actually `substring` finds 4 digits. If "Unknown", substring returns null.
        self.assertEqual(self._extract_year_sql_logic(None, "Unknown"), None)

        # Case 5: Year field exists but is garbage (unlikely due to XML schema but possible in raw_data)
        # If "Year" is "202x", Gold layer regex `^\d+$` will fail.
        self.assertEqual(self._extract_year_sql_logic("202x", None), None)

        # Case 6: Null inputs
        self.assertEqual(self._extract_year_sql_logic(None, None), None)

    def _flatten_authors_sql_logic(self, authors_json: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Mimics logic in `gold_pubmed_authors.sql`.
        """
        results = []
        for author in authors_json:
            last_name = author.get("LastName")
            fore_name = author.get("ForeName")
            initials = author.get("Initials")

            # Affiliation Logic
            # case when jsonb_typeof(AffiliationInfo) = 'array' then ... -> 0 ->> 'Affiliation'
            # when ... = 'object' then ... ->> 'Affiliation'
            aff_info = author.get("AffiliationInfo")
            affiliation = None

            if isinstance(aff_info, list) and len(aff_info) > 0:
                # In parsed JSON, item is dict
                affiliation = aff_info[0].get("Affiliation")
            elif isinstance(aff_info, dict):
                affiliation = aff_info.get("Affiliation")

            results.append(
                {
                    "last_name": last_name,
                    "fore_name": fore_name,
                    "initials": initials,
                    "affiliation": affiliation,
                }
            )
        return results

    def test_author_flattening(self) -> None:
        # Input Data
        authors = [
            # Case 1: Simple Author
            {
                "LastName": "Doe",
                "ForeName": "John",
                "Initials": "JD",
                "AffiliationInfo": {"Affiliation": "University of Life"},
            },
            # Case 2: List Affiliation
            {
                "LastName": "Smith",
                "AffiliationInfo": [
                    {"Affiliation": "Primary Lab"},
                    {"Affiliation": "Secondary Lab"},
                ],
            },
            # Case 3: No Affiliation
            {"LastName": "Unknown"},
        ]

        expected = [
            {
                "last_name": "Doe",
                "fore_name": "John",
                "initials": "JD",
                "affiliation": "University of Life",
            },
            {
                "last_name": "Smith",
                "fore_name": None,
                "initials": None,
                "affiliation": "Primary Lab",
            },
            {
                "last_name": "Unknown",
                "fore_name": None,
                "initials": None,
                "affiliation": None,
            },
        ]

        self.assertEqual(self._flatten_authors_sql_logic(authors), expected)

    def _flatten_mesh_sql_logic(self, mesh_terms_json: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Mimics logic in `gold_pubmed_mesh.sql`.
        """
        results = []
        for term in mesh_terms_json:
            desc_node = term.get("DescriptorName")
            descriptor_name = None
            descriptor_ui = None

            if isinstance(desc_node, dict):
                descriptor_name = desc_node.get("#text")
                descriptor_ui = desc_node.get("@UI")
            else:
                descriptor_name = desc_node

            results.append({"descriptor_name": descriptor_name, "descriptor_ui": descriptor_ui})
        return results

    def test_mesh_flattening(self) -> None:
        mesh = [
            # Case 1: Object with UI
            {"DescriptorName": {"#text": "Brain", "@UI": "D001921"}},
            # Case 2: Simple String (unlikely in this XML schema but defensive check)
            {"DescriptorName": "Neurons"},
        ]

        expected = [
            {"descriptor_name": "Brain", "descriptor_ui": "D001921"},
            {"descriptor_name": "Neurons", "descriptor_ui": None},
        ]

        self.assertEqual(self._flatten_mesh_sql_logic(mesh), expected)
