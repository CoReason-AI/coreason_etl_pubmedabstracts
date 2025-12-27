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
from unittest.mock import MagicMock

from coreason_etl_pubmedabstracts.pipelines.deduplication import run_deduplication_sweep


class TestDeduplication(unittest.TestCase):
    def test_run_deduplication_sweep(self) -> None:
        """Test that the deduplication SQL is generated and executed correctly."""
        # Mock the pipeline and sql_client
        mock_pipeline = MagicMock()
        mock_pipeline.dataset_name = "pubmed_data"

        mock_client = MagicMock()
        mock_pipeline.sql_client.return_value.__enter__.return_value = mock_client

        run_deduplication_sweep(mock_pipeline)

        # Verify SQL execution
        mock_client.execute_sql.assert_called_once()
        sql_arg = mock_client.execute_sql.call_args[0][0]

        # Check expected SQL parts
        self.assertIn("DELETE FROM pubmed_data.bronze_pubmed_updates", sql_arg)
        # Check for the robust CASE statement logic
        self.assertIn("CASE", sql_arg)
        self.assertIn("jsonb_typeof", sql_arg)
        self.assertIn("FROM pubmed_data.bronze_pubmed_baseline", sql_arg)

    def test_deduplication_failure(self) -> None:
        """Test error handling during deduplication."""
        mock_pipeline = MagicMock()
        mock_client = MagicMock()
        mock_pipeline.sql_client.return_value.__enter__.return_value = mock_client

        # Simulate an DB error
        mock_client.execute_sql.side_effect = Exception("DB Connection Failed")

        # Use assertRaisesRegex to satisfy B017 and verify the specific error
        with self.assertRaisesRegex(Exception, "DB Connection Failed"):
            run_deduplication_sweep(mock_pipeline)

    def test_empty_tables(self) -> None:
        """
        Verify that running sweep on empty tables does not raise errors.
        (Simulated by successful execution of SQL).
        """
        mock_pipeline = MagicMock()
        mock_pipeline.dataset_name = "pubmed_data"
        mock_client = MagicMock()
        mock_pipeline.sql_client.return_value.__enter__.return_value = mock_client

        # execute_sql returns None usually for DELETE, or row count.
        # Ensure no exception is raised.
        run_deduplication_sweep(mock_pipeline)
        mock_client.execute_sql.assert_called_once()

    def test_no_overlap(self) -> None:
        """
        Verify behavior when there is no overlap (DELETE touches 0 rows).
        """
        mock_pipeline = MagicMock()
        mock_pipeline.dataset_name = "pubmed_data"
        mock_client = MagicMock()
        mock_pipeline.sql_client.return_value.__enter__.return_value = mock_client

        run_deduplication_sweep(mock_pipeline)
        mock_client.execute_sql.assert_called_once()
