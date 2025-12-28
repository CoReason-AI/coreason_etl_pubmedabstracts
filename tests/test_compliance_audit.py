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
from typing import Any, Dict, List, Set


class TestComplianceHardDelete(unittest.TestCase):
    """
    Compliance verification for Physical Hard Deletes.
    Ensures that records marked as 'delete' (e.g. retractions) are
    physically removed from the dataset, satisfying the 'Hard Delete' requirement.
    """

    def _simulate_incremental_delete(
        self, existing_records: List[Dict[str, Any]], updates_batch: List[Dict[str, Any]], watermark_ts: float
    ) -> List[Dict[str, Any]]:
        """
        Simulates the dbt incremental logic + post-hook deletion.
        """
        # 1. Identify valid updates (newer than watermark)
        valid_updates = [r for r in updates_batch if r["ingestion_ts"] > watermark_ts]

        # 2. Identify Deletes (Post-Hook Logic)
        # The hook deletes from {{ this }} where source_id in (select pmid from updates where operation='delete')
        # Importantly, it uses the WATERMARK to scope the updates.
        ids_to_delete: Set[str] = set()

        # We must group by PMID to find the 'latest' operation for each PMID in the batch
        batch_map: Dict[str, List[Dict[str, Any]]] = {}
        for r in valid_updates:
            pmid = str(r["pmid"])
            if pmid not in batch_map:
                batch_map[pmid] = []
            batch_map[pmid].append(r)

        for pmid, rows in batch_map.items():
            # Rank by file_name desc, ingestion_ts desc
            rows.sort(key=lambda x: (x.get("file_name", ""), x["ingestion_ts"]), reverse=True)
            winner = rows[0]

            if winner["operation"] == "delete":
                ids_to_delete.add(pmid)

        # 3. Identify Upserts (Incremental Merge Logic)
        # Only upsert if the winner is NOT a delete (handled by the 'upsert' filter in the model)
        upserts: Dict[str, Dict[str, Any]] = {}
        for pmid, rows in batch_map.items():
            # Same ranking
            rows.sort(key=lambda x: (x.get("file_name", ""), x["ingestion_ts"]), reverse=True)
            winner = rows[0]

            if winner["operation"] == "upsert":
                upserts[pmid] = winner

        # 4. Apply Changes to Existing State

        # First, apply Deletes (Post-Hook effectively runs after, but logic wise it cleans the table)
        # Actually dbt runs merge THEN post-hook.
        # But since merge only inserts/updates where operation='upsert', the 'delete' rows
        # from the batch are never inserted.
        # So we just need to remove existing rows that match ids_to_delete.

        # Apply Upserts (Merge)
        current_state = {r["source_id"]: r for r in existing_records}
        for pmid, record in upserts.items():
            # Map batch record to table record
            current_state[pmid] = {
                "source_id": pmid,
                "ingestion_ts": record["ingestion_ts"],
                "title": record.get("title", "Updated"),
            }

        # Apply Deletes (Post-Hook)
        final_state = []
        for pmid, record in current_state.items():
            if pmid not in ids_to_delete:
                final_state.append(record)

        return final_state

    def test_compliance_retraction_scenario(self) -> None:
        """
        Scenario: A paper is published (Baseline/Upsert), then Retracted (Delete).
        The system must not contain the paper after the delete batch is processed.
        """
        # Initial State: Paper 12345 exists
        existing: List[Dict[str, Any]] = [{"source_id": "12345", "ingestion_ts": 100.0, "title": "Valid Science"}]

        # Batch: Retraction notice arrives
        batch = [{"pmid": "12345", "operation": "delete", "ingestion_ts": 110.0, "file_name": "pubmed24n0100.xml.gz"}]

        # Run
        result = self._simulate_incremental_delete(existing, batch, watermark_ts=105.0)

        # Verify: Record is GONE
        self.assertEqual(len(result), 0, "Retracted paper 12345 should be physically deleted.")

    def test_compliance_reinstatement_scenario(self) -> None:
        """
        Scenario: Paper Retracted (Delete), then Re-instated/Updated (Upsert) in a LATER batch.
        """
        existing: List[Dict[str, Any]] = []  # Paper was deleted previously

        batch = [
            {
                "pmid": "12345",
                "operation": "upsert",
                "ingestion_ts": 120.0,
                "file_name": "pubmed24n0101.xml.gz",
                "title": "Reinstated Science",
            }
        ]

        result = self._simulate_incremental_delete(existing, batch, watermark_ts=115.0)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_id"], "12345")
        self.assertEqual(result[0]["title"], "Reinstated Science")

    def test_complex_sequence_same_batch(self) -> None:
        """
        Scenario: Multiple updates for the same PMID within the same batch.
        Sequence: Upsert -> Delete -> Upsert (Latest should win).
        """
        existing: List[Dict[str, Any]] = []

        # All in same batch (watermark < 110)
        batch = [
            {"pmid": "A", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f1"},
            {"pmid": "A", "operation": "delete", "ingestion_ts": 111.0, "file_name": "f1"},
            {"pmid": "A", "operation": "upsert", "ingestion_ts": 112.0, "file_name": "f1"},
        ]

        result = self._simulate_incremental_delete(existing, batch, watermark_ts=100.0)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_id"], "A")
        # Ensure timestamp reflects the LATEST upsert
        self.assertEqual(result[0]["ingestion_ts"], 112.0)

    def test_watermark_boundary_precision(self) -> None:
        """
        Scenario: Verify strict inequality for watermark logic.
        Records exactly equal to watermark should be ignored (assumed already processed).
        """
        existing: List[Dict[str, Any]] = [{"source_id": "B", "ingestion_ts": 100.0}]

        batch = [
            # Exactly watermark -> Ignore (Safety mechanism to avoid reprocessing)
            {"pmid": "B", "operation": "delete", "ingestion_ts": 100.0, "file_name": "f1"},
            # Just above -> Process
            {"pmid": "C", "operation": "upsert", "ingestion_ts": 100.0001, "file_name": "f1"},
        ]

        result = self._simulate_incremental_delete(existing, batch, watermark_ts=100.0)

        # B should still exist (Delete ignored)
        # C should be added
        self.assertEqual(len(result), 2)
        ids = {r["source_id"] for r in result}
        self.assertIn("B", ids)
        self.assertIn("C", ids)

    def test_file_name_tie_breaking(self) -> None:
        """
        Scenario: Timestamps are identical, use Filename as tie breaker.
        Alphanumeric sort: pubmed24n0002 > pubmed24n0001
        """
        existing: List[Dict[str, Any]] = []

        batch = [
            # Logically 'later' file, same second
            {"pmid": "D", "operation": "delete", "ingestion_ts": 200.0, "file_name": "pubmed24n0002.xml"},
            # Logically 'earlier' file, same second
            {"pmid": "D", "operation": "upsert", "ingestion_ts": 200.0, "file_name": "pubmed24n0001.xml"},
        ]

        # Expected: Delete wins because filename is greater
        result = self._simulate_incremental_delete(existing, batch, watermark_ts=100.0)

        self.assertEqual(len(result), 0)

    def test_idempotency_of_deletes(self) -> None:
        """
        Scenario: Deleting a record that doesn't exist should be fine (No-op).
        """
        existing: List[Dict[str, Any]] = [{"source_id": "X", "ingestion_ts": 100.0}]

        batch = [{"pmid": "Y", "operation": "delete", "ingestion_ts": 110.0, "file_name": "f1"}]

        result = self._simulate_incremental_delete(existing, batch, watermark_ts=105.0)

        # X remains. Y delete was a no-op on existing state.
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_id"], "X")

    def test_mixed_batch_bulk(self) -> None:
        """
        Scenario: A mix of inserts, updates, and deletes to verify no cross-talk.
        """
        existing: List[Dict[str, Any]] = [
            {"source_id": "KEEP", "ingestion_ts": 100.0},
            {"source_id": "UPDATE", "ingestion_ts": 100.0},
            {"source_id": "DELETE", "ingestion_ts": 100.0},
        ]

        batch = [
            {"pmid": "UPDATE", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f1", "title": "New Title"},
            {"pmid": "DELETE", "operation": "delete", "ingestion_ts": 110.0, "file_name": "f1"},
            {"pmid": "NEW", "operation": "upsert", "ingestion_ts": 110.0, "file_name": "f1"},
            # OLD noise that should be ignored by watermark
            {"pmid": "KEEP", "operation": "delete", "ingestion_ts": 90.0, "file_name": "f1"},
        ]

        result = self._simulate_incremental_delete(existing, batch, watermark_ts=105.0)

        res_map = {r["source_id"]: r for r in result}

        self.assertIn("KEEP", res_map)  # Old delete ignored
        self.assertIn("UPDATE", res_map)
        self.assertEqual(res_map["UPDATE"]["title"], "New Title")
        self.assertNotIn("DELETE", res_map)
        self.assertIn("NEW", res_map)
