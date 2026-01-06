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
from typing import Any, Dict, List, Union

from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


class TestSqlLogic(unittest.TestCase):
    """
    These tests verify that the Python parser produces the exact JSON structure
    expected by the SQL logic, especially for edge cases handled by COALESCE/CASE.
    """

    def test_missing_elocation_id(self) -> None:
        """
        Verify that a missing ELocationID tag results in the key being absent
        (or None), which the SQL `coalesce(..., '[]'::jsonb)` must handle.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>2001</PMID>
                <Article>
                    <ArticleTitle>No DOI Paper</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        article = records[0]["MedlineCitation"]["Article"]
        # xmltodict simply omits the key if the tag is missing
        self.assertNotIn("ELocationID", article)

        # In SQL: raw_data -> ... -> 'ELocationID' will be NULL.
        # coalesce(NULL, '[]'::jsonb) -> '[]'.
        # jsonb_array_elements('[]') -> returns 0 rows.
        # Subquery returns NULL.
        # Correct behavior.

    def test_medline_date_parsing(self) -> None:
        """
        Verify the structure of MedlineDate for SQL regex extraction.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>2002</PMID>
                <Article>
                    <Journal>
                        <JournalIssue>
                            <PubDate>
                                <MedlineDate>1998 Dec-1999 Jan</MedlineDate>
                            </PubDate>
                        </JournalIssue>
                    </Journal>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        pub_date = records[0]["MedlineCitation"]["Article"]["Journal"]["JournalIssue"]["PubDate"]
        self.assertNotIn("Year", pub_date)
        self.assertIn("MedlineDate", pub_date)
        self.assertEqual(pub_date["MedlineDate"], "1998 Dec-1999 Jan")

        # In SQL:
        # raw_data -> ... -> 'Year' IS NULL.
        # raw_data -> ... -> 'MedlineDate' IS "1998 Dec-1999 Jan".
        # substring("1998 Dec-1999 Jan" from '\d{4}') -> "1998".
        # Correct behavior.

    def test_medline_date_mixed(self) -> None:
        """
        Verify behavior when MedlineDate is weird (e.g. "Spring 2000").
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>2003</PMID>
                <Article>
                    <Journal>
                        <JournalIssue>
                            <PubDate>
                                <MedlineDate>Spring 2000</MedlineDate>
                            </PubDate>
                        </JournalIssue>
                    </Journal>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))
        pub_date = records[0]["MedlineCitation"]["Article"]["Journal"]["JournalIssue"]["PubDate"]
        self.assertEqual(pub_date["MedlineDate"], "Spring 2000")
        # SQL substring should find "2000".


class TestStagingLayerJsonLogic(unittest.TestCase):
    """
    Tests the logic used in Staging Layer SQL (stg_pubmed_baseline.sql, stg_pubmed_updates.sql)
    to handle the JSONB variants produced by XML parsing.
    """

    def _simulate_pmid_extraction(self, pmid_node: Any) -> str:
        first_item = pmid_node[0]
        if isinstance(first_item, str):
            return first_item
        else:
            return str(first_item["#text"])

    def _simulate_title_extraction(self, title_node: Any) -> str:
        if isinstance(title_node, str):
            return title_node
        return str(title_node["#text"])

    def _simulate_abstract_extraction(self, abstract_node: Any) -> str:
        if isinstance(abstract_node, str):
            return abstract_node
        elif isinstance(abstract_node, list):
            parts = []
            for item in abstract_node:
                if isinstance(item, str):
                    parts.append(item)
                else:
                    parts.append(item["#text"])
            return " ".join(parts)
        elif abstract_node is None:
            return ""
        else:
            return str(abstract_node["#text"])

    def _simulate_doi_extraction(self, elocation_node: Any) -> Union[str, None]:
        """
        Simulate the SQL logic for extracting DOI from ELocationID array.
        SQL: select item->>'#text' from jsonb_array_elements(...) where item->>'@EIdType' = 'doi' limit 1
        """
        if not elocation_node:
            return None

        # In SQL, we coalesce to '[]' if null, so if elocation_node is None we return None (done above).
        # We assume input is a list because FORCE_LIST_KEYS ensures it is a list if present.
        if isinstance(elocation_node, list):
            for item in elocation_node:
                # Handle polymorphic item: string or dict
                # If string, it has no attributes, so @EIdType is null -> no match.
                if isinstance(item, dict):
                    eid_type = item.get("@EIdType")
                    if eid_type == "doi":
                        return item.get("#text")
        return None

    def test_pmid_simple_string(self) -> None:
        data = ["12345"]
        result = self._simulate_pmid_extraction(data)
        self.assertEqual(result, "12345")

    def test_pmid_with_attributes(self) -> None:
        data = [{"#text": "12345", "@Version": "1"}]
        result = self._simulate_pmid_extraction(data)
        self.assertEqual(result, "12345")

    def test_article_title_simple(self) -> None:
        data = "Hello World"
        result = self._simulate_title_extraction(data)
        self.assertEqual(result, "Hello World")

    def test_article_title_complex(self) -> None:
        data = {"#text": "Hello World", "@lang": "eng"}
        result = self._simulate_title_extraction(data)
        self.assertEqual(result, "Hello World")

    def test_abstract_list_mixed(self) -> None:
        data: List[Union[str, Dict[str, str]]] = [
            {"#text": "Text 1", "@Label": "BACKGROUND"},
            "Text 2",
        ]
        result = self._simulate_abstract_extraction(data)
        self.assertEqual(result, "Text 1 Text 2")

    # --- New Edge Cases for JSON Logic ---

    def test_mixed_type_list_extraction(self) -> None:
        """Verify list containing strings, objects, and nested structures is handled gracefully."""
        data: List[Union[str, Dict[str, str]]] = [
            "Intro",
            {"#text": "Details", "@Attr": "val"},
            "Conclusion",
        ]
        result = self._simulate_abstract_extraction(data)
        self.assertEqual(result, "Intro Details Conclusion")

    def test_unicode_preservation(self) -> None:
        """Verify that unicode characters (emojis, accents) are preserved."""
        data = "Beyoncé 🎸"
        result = self._simulate_title_extraction(data)
        self.assertEqual(result, "Beyoncé 🎸")

    def test_empty_or_null_inputs(self) -> None:
        """Ensure robustness when fields are None or empty lists."""
        # None
        self.assertEqual(self._simulate_abstract_extraction(None), "")
        # Empty list
        self.assertEqual(self._simulate_abstract_extraction([]), "")

    # --- Added Complex Edge Cases ---

    def test_doi_extraction_complex(self) -> None:
        """
        Verify finding a DOI hidden in a list of other ELocationIDs.
        Simulates: ELocationID is a list with [pii, doi, other].
        """
        data = [
            {"#text": "S0000-0000(00)00000-0", "@EIdType": "pii"},
            {"#text": "10.1016/j.test.2023.01.001", "@EIdType": "doi"},
            {"#text": "AnotherID", "@EIdType": "publisher-id"},
        ]
        result = self._simulate_doi_extraction(data)
        self.assertEqual(result, "10.1016/j.test.2023.01.001")

    def test_doi_extraction_no_doi(self) -> None:
        """Verify correct None return when no DOI is present in the list."""
        data = [
            {"#text": "S0000-0000(00)00000-0", "@EIdType": "pii"},
        ]
        result = self._simulate_doi_extraction(data)
        self.assertIsNone(result)

    def test_abstract_complex_construction(self) -> None:
        """
        Verify correct string concatenation when AbstractText is a mix of simple strings
        and labelled sections (dictionaries).
        This mimics <AbstractText Label="BACKGROUND">...</AbstractText> <AbstractText>...</AbstractText>
        """
        data: List[Union[str, Dict[str, str]]] = [
            {"#text": "Background info.", "@Label": "BACKGROUND"},
            "More info.",
            {"#text": "Methods info.", "@Label": "METHODS"},
        ]
        result = self._simulate_abstract_extraction(data)
        self.assertEqual(result, "Background info. More info. Methods info.")


class TestPhysicalHardDeleteLogic(unittest.TestCase):
    """
    Tests for the Physical Hard Delete logic implemented in int_pubmed_deduped.sql.
    """

    def _simulate_dbt_run(
        self, current_table: List[Dict[str, Any]], incoming_batch: List[Dict[str, Any]], max_ts_in_table: float
    ) -> List[Dict[str, Any]]:
        pre_hook_watermark = max_ts_in_table
        new_records = [r for r in incoming_batch if r["ingestion_ts"] > pre_hook_watermark]

        if not new_records:
            return current_table

        batch_grouped: Dict[str, List[Dict[str, Any]]] = {}
        for r in new_records:
            pmid = str(r["pmid"])
            if pmid not in batch_grouped:
                batch_grouped[pmid] = []
            batch_grouped[pmid].append(r)

        upserts_to_apply = []
        for _pmid, rows in batch_grouped.items():
            rows.sort(key=lambda x: (x.get("file_name", ""), x["ingestion_ts"]), reverse=True)
            winner = rows[0]
            if winner["operation"] == "upsert":
                upserts_to_apply.append(winner)

        table_map = {str(r["source_id"]): r for r in current_table}
        for up in upserts_to_apply:
            target_record = {
                "source_id": str(up["pmid"]),
                "ingestion_ts": up["ingestion_ts"],
                "file_name": up.get("file_name", ""),
                "title": up.get("title", "Updated Title"),
            }
            table_map[target_record["source_id"]] = target_record

        ids_to_delete = set()
        for pmid, rows in batch_grouped.items():
            rows.sort(key=lambda x: (x.get("file_name", ""), x["ingestion_ts"]), reverse=True)
            winner = rows[0]
            if winner["operation"] == "delete":
                ids_to_delete.add(pmid)

        final_table = []
        for source_id, record in table_map.items():
            if source_id not in ids_to_delete:
                final_table.append(record)

        return final_table

    def test_upsert_logic(self) -> None:
        current = [{"source_id": "1", "ingestion_ts": 100.0, "title": "Old"}]
        batch = [
            {
                "pmid": "1",
                "operation": "upsert",
                "ingestion_ts": 110.0,
                "file_name": "f2",
                "title": "New",
            },
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "New")

    def test_delete_existing(self) -> None:
        current = [{"source_id": "1", "ingestion_ts": 100.0, "title": "To Delete"}]
        batch = [
            {
                "pmid": "1",
                "operation": "delete",
                "ingestion_ts": 110.0,
                "file_name": "f2",
            }
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)
        self.assertEqual(len(result), 0)

    # ... (Keeping existing standard tests from previous step) ...
    def test_delete_then_upsert_in_batch(self) -> None:
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [
            {"pmid": "1", "operation": "delete", "ingestion_ts": 110.0, "file_name": "f2"},
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 111.0, "file_name": "f2"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["ingestion_ts"], 111.0)

    def test_upsert_then_delete_in_batch(self) -> None:
        current: List[Dict[str, Any]] = []
        batch = [
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2"},
            {"pmid": "1", "operation": "delete", "ingestion_ts": 111.0, "file_name": "f2"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        self.assertEqual(len(result), 0)

    def test_stale_update_ignored(self) -> None:
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [{"pmid": "1", "operation": "delete", "ingestion_ts": 90.0, "file_name": "f1"}]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=95.0)
        self.assertEqual(len(result), 1)

    def test_complex_sequence_in_batch(self) -> None:
        current: List[Dict[str, Any]] = []
        batch = [
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2"},
            {"pmid": "1", "operation": "delete", "ingestion_ts": 111.0, "file_name": "f2"},
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 112.0, "file_name": "f2"},
            {"pmid": "1", "operation": "delete", "ingestion_ts": 113.0, "file_name": "f2"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        self.assertEqual(len(result), 0)

    def test_watermark_boundary(self) -> None:
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [
            {"pmid": "1", "operation": "delete", "ingestion_ts": 100.0, "file_name": "f2"},
            {"pmid": "2", "operation": "upsert", "ingestion_ts": 101.0, "file_name": "f2"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        self.assertEqual(len(result), 2)

    def test_tie_breaking_same_ts(self) -> None:
        current: List[Dict[str, Any]] = []
        batch = [
            {"pmid": "1", "operation": "delete", "ingestion_ts": 110.0, "file_name": "pub24n0001"},
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "pub24n0002"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["file_name"], "pub24n0002")

    def test_large_mixed_batch(self) -> None:
        current = [{"source_id": str(i), "ingestion_ts": 100.0} for i in range(100)]
        batch = []
        for i in range(50):
            batch.append({"pmid": str(i), "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2"})
        for i in range(50, 100):
            batch.append({"pmid": str(i), "operation": "delete", "ingestion_ts": 110.0, "file_name": "f2"})
        for i in range(100, 150):
            batch.append({"pmid": str(i), "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2"})

        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)
        self.assertEqual(len(result), 100)
        res_map = {r["source_id"]: r for r in result}
        self.assertEqual(res_map["0"]["ingestion_ts"], 110.0)
        self.assertNotIn("50", res_map)
        self.assertIn("100", res_map)

    # --- New Edge Cases for Hard Delete Logic ---

    def test_redundant_deletes(self) -> None:
        """
        Multiple delete operations for the same PMID in a single batch.
        Should result in deletion (idempotent).
        """
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [
            {"pmid": "1", "operation": "delete", "ingestion_ts": 110.0, "file_name": "f1"},
            {"pmid": "1", "operation": "delete", "ingestion_ts": 111.0, "file_name": "f2"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)
        self.assertEqual(len(result), 0)

    def test_delete_non_existent(self) -> None:
        """
        Deleting a PMID not in the current table.
        Should handle gracefully (no error, no change).
        """
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [
            {"pmid": "99", "operation": "delete", "ingestion_ts": 110.0, "file_name": "f1"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_id"], "1")

    def test_floating_point_precision(self) -> None:
        """
        Verify watermark behavior with very close timestamps.
        """
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [
            # Just barely above watermark
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 100.000001, "file_name": "f1", "title": "New"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["title"], "New")

    def test_cross_batch_idempotency(self) -> None:
        """
        Re-running the same batch should not change the state if watermark advances.
        Wait, if watermark advances, the batch is filtered out.
        If we re-run with SAME watermark?
        """
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [{"pmid": "1", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f1"}]

        # Run 1
        result1 = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        self.assertEqual(len(result1), 1)
        self.assertEqual(result1[0]["ingestion_ts"], 110.0)

        # Run 2 (Simulating next run, watermark is now 110.0)
        # Batch items (ts=110.0) are NOT > 110.0, so they are filtered out.
        result2 = self._simulate_dbt_run(result1, batch, max_ts_in_table=110.0)
        self.assertEqual(len(result2), 1)
        # State should be identical
        self.assertEqual(result2, result1)


class TestDateLogic(unittest.TestCase):
    """
    Tests the logic for constructing publication_date from partial XML fields.
    This mirrors the intended SQL logic in int_pubmed_deduped.sql.
    """

    def _simulate_sql_date_logic(self, year: str, month: str, day: str, medline_date: str) -> str:
        """
        Simulate the SQL CASE logic for date construction.
        Returns a string 'YYYY-MM-DD'.
        """
        # 1. Clean/Normalize inputs (SQL: coalesce(..., ''))
        y = year if year else ""
        m = month if month else ""
        d = day if day else ""
        md = medline_date if medline_date else ""

        # 2. Extract Year
        final_year = y
        if not final_year and md:
            # SQL: substring(medline_date from '\d{4}')
            import re

            match = re.search(r"\d{4}", md)
            if match:
                final_year = match.group(0)

        if not final_year:
            return "1900-01-01"  # Default fallback

        # 3. Extract Month
        final_month = "01"
        if m:
            # SQL: Case insensitive matching for months
            m_lower = m.lower()
            months = {
                "jan": "01",
                "january": "01",
                "feb": "02",
                "february": "02",
                "mar": "03",
                "march": "03",
                "apr": "04",
                "april": "04",
                "may": "05",
                "jun": "06",
                "june": "06",
                "jul": "07",
                "july": "07",
                "aug": "08",
                "august": "08",
                "sep": "09",
                "september": "09",
                "oct": "10",
                "october": "10",
                "nov": "11",
                "november": "11",
                "dec": "12",
                "december": "12",
            }
            # Also handle digit months "01", "1", etc.
            if m_lower in months:
                final_month = months[m_lower]
            elif m.isdigit():
                final_month = f"{int(m):02d}"
        elif md:
            # Try to find month in MedlineDate if Year was found there (or even if not)
            # MedlineDate examples: "1998 Dec-1999 Jan", "Spring 2000"
            # Logic: If we rely on MedlineDate, we usually just default to Jan unless we want to be fancy.
            # For this iteration, let's keep it simple: If using MedlineDate, default to Jan 01
            # unless we want to parse it. The requirement says "extract publication_date".
            # Standard practice: First day of the year/season.
            pass

        # 4. Extract Day
        final_day = "01"
        if d and d.isdigit():
            final_day = f"{int(d):02d}"

        return f"{final_year}-{final_month}-{final_day}"

    def test_standard_date(self) -> None:
        self.assertEqual(self._simulate_sql_date_logic("2023", "May", "15", ""), "2023-05-15")

    def test_numeric_date(self) -> None:
        self.assertEqual(self._simulate_sql_date_logic("2023", "05", "15", ""), "2023-05-15")

    def test_short_month(self) -> None:
        self.assertEqual(self._simulate_sql_date_logic("2023", "Dec", "01", ""), "2023-12-01")

    def test_medline_date_year_only(self) -> None:
        self.assertEqual(self._simulate_sql_date_logic("", "", "", "1998 Dec-1999 Jan"), "1998-01-01")

    def test_medline_date_season(self) -> None:
        self.assertEqual(self._simulate_sql_date_logic("", "", "", "Spring 2000"), "2000-01-01")

    def test_fallback(self) -> None:
        self.assertEqual(self._simulate_sql_date_logic("", "", "", ""), "1900-01-01")

    # --- Added Complex Edge Cases for Dates ---

    def test_pubyear_logic_precedence(self) -> None:
        """
        Verify that an explicit Year tag takes precedence over MedlineDate.
        XML: <Year>2023</Year> <MedlineDate>2020 Spring</MedlineDate>
        Expected: 2023
        """
        self.assertEqual(self._simulate_sql_date_logic("2023", "", "", "2020 Spring"), "2023-01-01")

    def test_pubyear_medline_regex_complex(self) -> None:
        """
        Verify regex extraction from complex MedlineDate strings.
        """
        # Case: Range across years (take first)
        self.assertEqual(self._simulate_sql_date_logic("", "", "", "2020-2021"), "2020-01-01")
        # Case: Text before year
        self.assertEqual(self._simulate_sql_date_logic("", "", "", "Published 2020"), "2020-01-01")
