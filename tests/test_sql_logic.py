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
from typing import Any, Dict, List

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


class TestPhysicalHardDeleteLogic(unittest.TestCase):
    """
    Tests for the Physical Hard Delete logic implemented in int_pubmed_deduped.sql.
    Simulates the `incremental` model update + `post-hook` behavior using Python logic.
    """

    def _simulate_dbt_run(
        self, current_table: List[Dict[str, Any]], incoming_batch: List[Dict[str, Any]], max_ts_in_table: float
    ) -> List[Dict[str, Any]]:
        """
        Simulates the dbt incremental model run:
        1. Capture Pre-Hook Watermark (max_ts_in_table).
        2. Identify new records (ingestion_ts > watermark).
        3. Apply Upserts (Merge).
        4. Apply Post-Hook (Delete) using the CAPTURED watermark.

        Args:
            current_table: List of records currently in the Silver table.
                           Each record must have 'source_id' and 'ingestion_ts'.
            incoming_batch: List of records from stg_pubmed_updates.
                           Each record must have 'pmid', 'operation', 'ingestion_ts', 'file_name'.
            max_ts_in_table: The watermark for incremental loading (Pre-Hook state).

        Returns:
            The state of the table after the run.
        """
        # Capture Watermark (Pre-Hook)
        pre_hook_watermark = max_ts_in_table

        # 0. Filter Incoming Batch (Incremental Logic)
        # In SQL: where ingestion_ts > (select max(ingestion_ts) from {{ this }})
        # Note: Ideally main query uses current max ts.
        new_records = [r for r in incoming_batch if r["ingestion_ts"] > pre_hook_watermark]

        # If no new records, nothing changes
        if not new_records:
            return current_table

        # ---------------------------------------------------------
        # Step 1: Main Query Logic (Identify Rows to Upsert)
        # ---------------------------------------------------------
        # Logic: Rank by (pmid, file_name desc, ingestion_ts desc)
        # Filter rn=1 and operation='upsert'

        batch_grouped: Dict[str, List[Dict[str, Any]]] = {}
        for r in new_records:
            pmid = str(r["pmid"])
            if pmid not in batch_grouped:
                batch_grouped[pmid] = []
            batch_grouped[pmid].append(r)

        upserts_to_apply = []
        for _pmid, rows in batch_grouped.items():
            # Rank: file_name desc, ingestion_ts desc
            rows.sort(key=lambda x: (x.get("file_name", ""), x["ingestion_ts"]), reverse=True)
            winner = rows[0]

            if winner["operation"] == "upsert":
                upserts_to_apply.append(winner)

        # ---------------------------------------------------------
        # Step 2: Apply Upserts (Merge)
        # ---------------------------------------------------------
        # In dbt incremental (unique_key='source_id'), this updates existing or inserts new.

        table_map = {str(r["source_id"]): r for r in current_table}

        for up in upserts_to_apply:
            target_record = {
                "source_id": str(up["pmid"]),
                "ingestion_ts": up["ingestion_ts"],
                "file_name": up.get("file_name", ""),
                "title": up.get("title", "Updated Title"),
                # ... other fields
            }
            table_map[target_record["source_id"]] = target_record

        # ---------------------------------------------------------
        # Step 3: Post-Hook Logic (Physical Delete)
        # ---------------------------------------------------------
        # Correct Logic: Use pre_hook_watermark, NOT the new max_ts from the table (which might be higher now)

        ids_to_delete = set()
        for pmid, rows in batch_grouped.items():
            # Same ranking logic as above
            rows.sort(key=lambda x: (x.get("file_name", ""), x["ingestion_ts"]), reverse=True)
            winner = rows[0]

            # Key check: Ensure we are processing the SAME batch as the main query
            # The filter logic in post-hook must match the filter logic in main query or better.
            # Using pre_hook_watermark ensures we see the deletes that came in this batch.

            if winner["operation"] == "delete":
                ids_to_delete.add(pmid)

        # Apply Deletes
        final_table = []
        for source_id, record in table_map.items():
            if source_id not in ids_to_delete:
                final_table.append(record)

        return final_table

    def test_upsert_logic(self) -> None:
        """Verify standard upsert behavior."""
        current = [{"source_id": "1", "ingestion_ts": 100.0, "title": "Old"}]
        batch = [
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2", "title": "New"},
            {"pmid": "2", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2", "title": "Fresh"},
        ]

        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)

        self.assertEqual(len(result), 2)
        res_map = {r["source_id"]: r for r in result}
        self.assertEqual(res_map["1"]["title"], "New")
        self.assertEqual(res_map["2"]["title"], "Fresh")

    def test_delete_existing(self) -> None:
        """Verify delete removes an existing record."""
        current = [{"source_id": "1", "ingestion_ts": 100.0, "title": "To Delete"}]
        batch = [{"pmid": "1", "operation": "delete", "ingestion_ts": 110.0, "file_name": "f2"}]

        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)
        self.assertEqual(len(result), 0)

    def test_delete_then_upsert_in_batch(self) -> None:
        """
        Verify that if a batch contains Delete then Upsert (Upsert is later),
        the record survives.
        """
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [
            {"pmid": "1", "operation": "delete", "ingestion_ts": 110.0, "file_name": "f2"},
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 111.0, "file_name": "f2"},
        ]

        # Max TS is 105. Both are new.
        # Winner is ts=111 (Upsert).
        # Post-hook sees Winner=Upsert, so it does NOT add to delete list.
        # Main query sees Winner=Upsert, so it ADDS to upsert list.

        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["ingestion_ts"], 111.0)

    def test_upsert_then_delete_in_batch(self) -> None:
        """
        Verify that if a batch contains Upsert then Delete (Delete is later),
        the record is removed.
        """
        current: List[Dict[str, Any]] = []
        batch = [
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2"},
            {"pmid": "1", "operation": "delete", "ingestion_ts": 111.0, "file_name": "f2"},
        ]

        # Winner is ts=111 (Delete).
        # Main query sees Winner=Delete (operation != upsert), so it does NOT upsert.
        # Post-hook sees Winner=Delete, so it ADDS to delete list.
        # Result: Record not in table.

        result = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        self.assertEqual(len(result), 0)

    def test_stale_update_ignored(self) -> None:
        """Verify that records older than watermark are ignored."""
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [{"pmid": "1", "operation": "delete", "ingestion_ts": 90.0, "file_name": "f1"}]

        result = self._simulate_dbt_run(current, batch, max_ts_in_table=95.0)
        # Batch ignored. Table remains.
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_id"], "1")

    def test_complex_sequence_in_batch(self) -> None:
        """
        Verify complex sequence: Upsert -> Delete -> Upsert -> Delete.
        Latest is Delete, so record should be removed.
        """
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
        """
        Verify strict inequality for watermark logic (ingestion_ts > max_ts).
        Record with ts equal to watermark should be ignored.
        """
        current = [{"source_id": "1", "ingestion_ts": 100.0}]
        batch = [
            # Equal to watermark: should be ignored
            {"pmid": "1", "operation": "delete", "ingestion_ts": 100.0, "file_name": "f2"},
            # Greater than watermark: should be processed
            {"pmid": "2", "operation": "upsert", "ingestion_ts": 101.0, "file_name": "f2"},
        ]
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        # 1 remains (delete ignored), 2 added
        self.assertEqual(len(result), 2)
        res_map = {r["source_id"]: r for r in result}
        self.assertEqual(res_map["1"]["ingestion_ts"], 100.0)
        self.assertEqual(res_map["2"]["ingestion_ts"], 101.0)

    def test_tie_breaking_same_ts(self) -> None:
        """
        Verify tie-breaking using filename when ingestion_ts is identical.
        File names sort alphanumerically: 'pub24n0002' > 'pub24n0001'.
        """
        current: List[Dict[str, Any]] = []
        batch = [
            # Older file (lexicographically)
            {"pmid": "1", "operation": "delete", "ingestion_ts": 110.0, "file_name": "pub24n0001"},
            # Newer file (lexicographically)
            {"pmid": "1", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "pub24n0002"},
        ]
        # Winner should be 'upsert' from pub24n0002
        result = self._simulate_dbt_run(current, batch, max_ts_in_table=100.0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["file_name"], "pub24n0002")

    def test_large_mixed_batch(self) -> None:
        """
        Verify performance and correctness on a larger mixed batch.
        """
        current = [{"source_id": str(i), "ingestion_ts": 100.0} for i in range(100)]
        batch = []
        # Update first 50
        for i in range(50):
            batch.append({"pmid": str(i), "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2"})
        # Delete next 50
        for i in range(50, 100):
            batch.append({"pmid": str(i), "operation": "delete", "ingestion_ts": 110.0, "file_name": "f2"})
        # Add 50 new
        for i in range(100, 150):
            batch.append({"pmid": str(i), "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f2"})

        result = self._simulate_dbt_run(current, batch, max_ts_in_table=105.0)

        self.assertEqual(len(result), 100)  # 50 updated + 50 new
        res_map = {r["source_id"]: r for r in result}
        # Check an updated one
        self.assertEqual(res_map["0"]["ingestion_ts"], 110.0)
        # Check a deleted one (should not exist)
        self.assertNotIn("50", res_map)
        # Check a new one
        self.assertIn("100", res_map)
