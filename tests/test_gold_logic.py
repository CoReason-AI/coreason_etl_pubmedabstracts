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
import uuid
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
        authors: List[Dict[str, Any]] = [
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

    def _generate_author_id_sql_logic(self, author: Dict[str, Any]) -> str:
        """
        Mimics the uuid_generate_v5 logic in gold_pubmed_authors.sql.
        Namespace: uuid.NAMESPACE_DNS (6ba7b810-9dad-11d1-80b4-00c04fd430c8)
        Key: LastName|ForeName|Initials
        """
        last_name = author.get("LastName") or ""
        fore_name = author.get("ForeName") or ""
        initials = author.get("Initials") or ""

        # Concatenate with separator
        name_key = f"{last_name}|{fore_name}|{initials}"

        # Generate UUIDv5
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, name_key))

    def test_author_identity_resolution(self) -> None:
        """Verify deterministic ID generation for authors."""
        # Case 1: Standard Author
        author_1 = {"LastName": "Doe", "ForeName": "John", "Initials": "JD"}
        uuid_1 = self._generate_author_id_sql_logic(author_1)

        # Verify Determinism (Same input -> Same UUID)
        uuid_1_retry = self._generate_author_id_sql_logic(author_1)
        self.assertEqual(uuid_1, uuid_1_retry)

        # Case 2: Partial Data (Null ForeName)
        author_2 = {"LastName": "Doe", "Initials": "JD"}  # ForeName missing
        uuid_2 = self._generate_author_id_sql_logic(author_2)

        # Verify it's different from Case 1
        self.assertNotEqual(uuid_1, uuid_2)

        # Case 3: Verify specific UUID value (Regression testing)
        # Name: "Doe|John|JD"
        # Namespace: DNS
        # Expected: generated externally or checked once
        expected_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, "Doe|John|JD"))
        self.assertEqual(uuid_1, expected_uuid)

        # Case 4: Empty Author (All nulls)
        author_empty: Dict[str, Any] = {}
        uuid_empty = self._generate_author_id_sql_logic(author_empty)
        expected_empty = str(uuid.uuid5(uuid.NAMESPACE_DNS, "||"))
        self.assertEqual(uuid_empty, expected_empty)

    def _generate_mesh_id_sql_logic(self, mesh_item: Dict[str, Any]) -> str:
        """
        Mimics uuid_generate_v5 logic in gold_pubmed_mesh.sql.
        Key: DescriptorUI if exists, else DescriptorName.
        """
        descriptor_node = mesh_item.get("DescriptorName")
        descriptor_ui = None
        descriptor_name = None

        if isinstance(descriptor_node, dict):
            descriptor_ui = descriptor_node.get("@UI")
            descriptor_name = descriptor_node.get("#text")
        else:
            descriptor_name = descriptor_node

        # Priority: UI > Name
        key_seed = descriptor_ui if descriptor_ui else descriptor_name

        # If both are null (unlikely but possible), this would fail in pure SQL unless handled.
        # The SQL uses COALESCE, so if both are null, it might be null or error depending on uuid function.
        # uuid_generate_v5 requires a string. If key is null, it returns null.
        if key_seed is None:
            return None  # type: ignore

        return str(uuid.uuid5(uuid.NAMESPACE_DNS, key_seed))

    def test_mesh_identity_resolution(self) -> None:
        """Verify deterministic ID generation for MeSH terms."""
        # Case 1: Standard MeSH with UI
        mesh_1 = {"DescriptorName": {"#text": "Brain", "@UI": "D001921"}}
        uuid_1 = self._generate_mesh_id_sql_logic(mesh_1)

        # Expected: UUIDv5(DNS, "D001921")
        expected_1 = str(uuid.uuid5(uuid.NAMESPACE_DNS, "D001921"))
        self.assertEqual(uuid_1, expected_1)

        # Case 2: MeSH without UI (Fallback to Name)
        mesh_2 = {"DescriptorName": "Neurons"}
        uuid_2 = self._generate_mesh_id_sql_logic(mesh_2)

        # Expected: UUIDv5(DNS, "Neurons")
        expected_2 = str(uuid.uuid5(uuid.NAMESPACE_DNS, "Neurons"))
        self.assertEqual(uuid_2, expected_2)
        self.assertNotEqual(uuid_1, uuid_2)

    def test_identity_resolution_edge_cases(self) -> None:
        """Verify robustness against Unicode, Whitespace, and Special Characters."""

        # --- Unicode Handling ---
        # Author: "Ménière"
        author_unicode = {"LastName": "Ménière", "ForeName": "P", "Initials": "P"}
        uuid_unicode_auth = self._generate_author_id_sql_logic(author_unicode)

        # Verify it generates a valid UUID and doesn't crash
        self.assertIsNotNone(uuid_unicode_auth)
        # Verify determinism
        self.assertEqual(uuid_unicode_auth, self._generate_author_id_sql_logic(author_unicode))

        # MeSH: "José"
        mesh_unicode = {"DescriptorName": "José"}
        uuid_unicode_mesh = self._generate_mesh_id_sql_logic(mesh_unicode)
        self.assertEqual(uuid_unicode_mesh, str(uuid.uuid5(uuid.NAMESPACE_DNS, "José")))

        # --- Whitespace Handling ---
        # " Smith " vs "Smith"
        # The current logic does NOT trim. Verify that.
        author_space = {"LastName": " Smith ", "ForeName": "John", "Initials": "JD"}
        author_clean = {"LastName": "Smith", "ForeName": "John", "Initials": "JD"}

        self.assertNotEqual(
            self._generate_author_id_sql_logic(author_space),
            self._generate_author_id_sql_logic(author_clean),
            "Whitespace should NOT be trimmed automatically, resulting in different IDs",
        )

        # --- Separator Collision (The Pipe Problem) ---
        # Author A: Last="Smith|Jones", Fore="Bob", Init="B" -> "Smith|Jones|Bob|B"
        # Author B: Last="Smith", Fore="Jones|Bob", Init="B" -> "Smith|Jones|Bob|B"
        # These WILL collide with the current logic.
        # We accept this risk for now, but we document it with this test.
        author_a = {"LastName": "Smith|Jones", "ForeName": "Bob", "Initials": "B"}
        author_b = {"LastName": "Smith", "ForeName": "Jones|Bob", "Initials": "B"}

        self.assertEqual(
            self._generate_author_id_sql_logic(author_a),
            self._generate_author_id_sql_logic(author_b),
            "Separator collision confirmed: Pipes in data mimic the separator.",
        )

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
        mesh: List[Dict[str, Any]] = [
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

    def _filter_language_sql_logic(self, languages_json: Optional[List[str]], filter_lang: Optional[str]) -> bool:
        """
        Mimics logic in `gold_pubmed_knowledge.sql` for language filtering.

        SQL Logic:
        exists (select 1 from ... where lang = FILTER_LANGUAGE)
        OR FILTER_LANGUAGE = ''
        OR FILTER_LANGUAGE IS NULL
        """
        # "OR FILTER_LANGUAGE = '' OR ... IS NULL"
        if filter_lang is None or filter_lang == "":
            return True

        # "coalesce(languages, '[]'::jsonb)"
        if not languages_json:
            return False

        # "exists (... where lang = FILTER_LANGUAGE)"
        return filter_lang in languages_json

    def test_language_filter(self) -> None:
        # Case 1: Match Found (Standard 'eng')
        self.assertTrue(self._filter_language_sql_logic(["eng", "fre"], "eng"))

        # Case 2: No Match
        self.assertFalse(self._filter_language_sql_logic(["fre", "spa"], "eng"))

        # Case 3: Empty Language List
        self.assertFalse(self._filter_language_sql_logic([], "eng"))

        # Case 4: None Language List (SQL coalesce handles this)
        self.assertFalse(self._filter_language_sql_logic(None, "eng"))

        # Case 5: Filter is Empty String (Should allow all)
        self.assertTrue(self._filter_language_sql_logic(["fre"], ""))

        # Case 6: Filter is None (Should allow all)
        self.assertTrue(self._filter_language_sql_logic(["fre"], None))

    def test_language_filter_complex(self) -> None:
        """Verify strictness of the language filter logic."""
        # Case 1: Case Sensitivity (SQL '=' is case sensitive)
        # 'Eng' should NOT match 'eng'
        self.assertFalse(self._filter_language_sql_logic(["Eng"], "eng"))

        # Case 2: Whitespace (SQL '=' does not trim automatically)
        self.assertFalse(self._filter_language_sql_logic(["eng "], "eng"))
        self.assertFalse(self._filter_language_sql_logic([" eng"], "eng"))

        # Case 3: Unicode Safety
        # Should handle match correctly
        self.assertTrue(self._filter_language_sql_logic(["français"], "français"))
        # Mismatch
        self.assertFalse(self._filter_language_sql_logic(["français"], "francais"))

        # Case 4: Duplicate entries in source list (should pass if one matches)
        self.assertTrue(self._filter_language_sql_logic(["eng", "eng", "fre"], "eng"))

    def _simulate_gold_filtering(self, records: List[Dict[str, Any]], filter_lang: str) -> List[Dict[str, Any]]:
        """
        Helper to mimic the WHERE clause filtering on a list of records.
        """
        return [r for r in records if self._filter_language_sql_logic(r.get("languages"), filter_lang)]

    def test_language_filter_exclusion(self) -> None:
        """
        Verify that records are correctly included/excluded based on the language filter.
        Strictly tests: Exclusion, Inclusion, and Null Safety (Null/Empty lists).
        """
        # Mock Records
        records: List[Dict[str, Any]] = [
            {"id": 1, "languages": ["eng"]},
            {"id": 2, "languages": ["fre"]},  # Exclude
            {"id": 3, "languages": ["eng", "fre"]},  # Include
            {"id": 4, "languages": []},  # Exclude (Null Safety)
            {"id": 5, "languages": None},  # Exclude (Null Safety)
            {"id": 6, "languages": ["spa"]},  # Exclude
        ]

        # Scenario 1: Filter = 'eng'
        filtered_eng = self._simulate_gold_filtering(records, "eng")
        ids_eng = [r["id"] for r in filtered_eng]

        # Assert correct inclusions
        self.assertIn(1, ids_eng)
        self.assertIn(3, ids_eng)

        # Assert correct exclusions (Strict checks)
        self.assertNotIn(2, ids_eng)
        self.assertNotIn(4, ids_eng)
        self.assertNotIn(5, ids_eng)
        self.assertNotIn(6, ids_eng)
        self.assertEqual(len(filtered_eng), 2)

        # Scenario 2: Filter = 'fre'
        filtered_fre = self._simulate_gold_filtering(records, "fre")
        ids_fre = [r["id"] for r in filtered_fre]

        self.assertIn(2, ids_fre)
        self.assertIn(3, ids_fre)
        self.assertNotIn(1, ids_fre)
        self.assertEqual(len(filtered_fre), 2)

        # Scenario 3: Filter = '' (Should include all)
        # Note: SQL Logic: OR FILTER_LANGUAGE = ''
        filtered_all = self._simulate_gold_filtering(records, "")
        self.assertEqual(len(filtered_all), 6)

    def test_publication_year_complex(self) -> None:
        """Verify year extraction resilience."""
        # Case 1: Year 0 (Regex \d+ matches, but typically invalid for analysis)
        # The logic extraction returns the int. Downstream SQL make_date might fail,
        # but the extraction logic itself simply casts.
        self.assertEqual(self._extract_year_sql_logic("0", None), 0)

        # Case 2: Year 9999
        self.assertEqual(self._extract_year_sql_logic("9999", None), 9999)

        # Case 3: Negative Year (Regex ^\d+$ does NOT match negative)
        self.assertIsNone(self._extract_year_sql_logic("-2000", None))

        # Case 4: Decimals (Regex ^\d+$ does NOT match)
        self.assertIsNone(self._extract_year_sql_logic("2000.0", None))

        # Case 5: Deeply nested/Malformed MedlineDate strings
        # "2023 Nov 15" -> finds 2023
        self.assertEqual(self._extract_year_sql_logic(None, "2023 Nov 15"), 2023)
        # "Nov 15, 2023" -> finds 2023
        self.assertEqual(self._extract_year_sql_logic(None, "Nov 15, 2023"), 2023)
        # "No digits here" -> None
        self.assertIsNone(self._extract_year_sql_logic(None, "No digits here"))

    def test_author_flattening_complex(self) -> None:
        """Verify author flattening handles partial/malformed data."""
        # Case 1: Empty dictionaries in list
        authors_missing_keys = [{}, {"LastName": "Doe"}]
        expected = [
            {"last_name": None, "fore_name": None, "initials": None, "affiliation": None},
            {"last_name": "Doe", "fore_name": None, "initials": None, "affiliation": None},
        ]
        self.assertEqual(self._flatten_authors_sql_logic(authors_missing_keys), expected)

        # Case 2: AffiliationInfo is malformed (not dict or list)
        authors_bad_aff = [{"LastName": "Bond", "AffiliationInfo": "Secret"}]
        # Logic expects list or dict. String should result in None for affiliation.
        result = self._flatten_authors_sql_logic(authors_bad_aff)
        self.assertEqual(result[0]["affiliation"], None)

    def test_mesh_flattening_complex(self) -> None:
        """Verify MeSH flattening resilience."""
        # Case 1: DescriptorName is missing or None
        mesh = [{"QualifierName": "Q1"}]
        result = self._flatten_mesh_sql_logic(mesh)
        self.assertIsNone(result[0]["descriptor_name"])
        self.assertIsNone(result[0]["descriptor_ui"])

        # Case 2: DescriptorName is empty dict
        mesh_empty: List[Dict[str, Any]] = [{"DescriptorName": {}}]
        result_empty = self._flatten_mesh_sql_logic(mesh_empty)
        self.assertIsNone(result_empty[0]["descriptor_name"])
