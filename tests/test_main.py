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
from unittest.mock import MagicMock, patch

from coreason_etl_pubmedabstracts.main import get_args, run_pipeline


class TestMainOrchestration(unittest.TestCase):
    def test_get_args(self) -> None:
        """Test argument parsing."""
        args = get_args(["--load", "baseline"])
        self.assertEqual(args.load, "baseline")
        self.assertFalse(args.dry_run)

        args = get_args(["--dry-run"])
        self.assertEqual(args.load, "all")
        self.assertTrue(args.dry_run)

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.run_deduplication_sweep")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    def test_run_pipeline_all(self, mock_source: MagicMock, mock_sweep: MagicMock, mock_pipeline: MagicMock) -> None:
        """Test running all resources triggers sweep."""
        # Setup mock pipeline
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance

        # Setup successful load info
        mock_info = MagicMock()
        mock_info.has_failed_jobs = False
        mock_p_instance.run.return_value = mock_info

        # Execute
        run_pipeline("all")

        # Verify pipeline init
        mock_pipeline.assert_called_once_with(
            pipeline_name="pubmed_abstracts",
            destination="postgres",
            dataset_name="pubmed",
            progress="log",
        )

        # Verify run called with both resources
        # dlt.run(source, table_name=...) logic
        # table_name argument matches our resources_to_run list
        mock_p_instance.run.assert_called_once()
        _, kwargs = mock_p_instance.run.call_args
        self.assertEqual(kwargs["table_name"], ["pubmed_baseline", "pubmed_updates"])

        # Verify sweep called
        mock_sweep.assert_called_once_with(mock_p_instance)

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.run_deduplication_sweep")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    def test_run_pipeline_updates_only(
        self, mock_source: MagicMock, mock_sweep: MagicMock, mock_pipeline: MagicMock
    ) -> None:
        """Test running only updates does NOT trigger sweep."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance
        mock_info = MagicMock()
        mock_info.has_failed_jobs = False
        mock_p_instance.run.return_value = mock_info

        run_pipeline("updates")

        # Verify run called with updates only
        _, kwargs = mock_p_instance.run.call_args
        self.assertEqual(kwargs["table_name"], ["pubmed_updates"])

        # Verify sweep NOT called
        mock_sweep.assert_not_called()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.run_deduplication_sweep")
    def test_dry_run(self, mock_sweep: MagicMock, mock_pipeline: MagicMock) -> None:
        """Test dry run skips execution."""
        run_pipeline("all", dry_run=True)

        mock_pipeline.assert_called_once()
        mock_pipeline.return_value.run.assert_not_called()
        mock_sweep.assert_not_called()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.sys.exit")
    def test_failed_jobs_exit(self, mock_exit: MagicMock, mock_pipeline: MagicMock) -> None:
        """Test that failed jobs trigger sys.exit(1)."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance

        mock_info = MagicMock()
        mock_info.has_failed_jobs = True
        mock_p_instance.run.return_value = mock_info

        run_pipeline("all")

        mock_exit.assert_called_once_with(1)
