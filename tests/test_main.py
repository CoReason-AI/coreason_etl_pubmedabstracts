# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import subprocess
import unittest
from unittest.mock import MagicMock, PropertyMock, patch

from coreason_etl_pubmedabstracts.main import get_args, main, run_dbt_transformations, run_pipeline


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
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_run_pipeline_all(
        self,
        mock_run_dbt: MagicMock,
        mock_source_func: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test running all resources triggers dbt."""
        # Setup mock pipeline
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance

        # Setup successful load info
        mock_info = MagicMock()
        mock_info.has_failed_jobs = False
        mock_p_instance.run.return_value = mock_info

        # Setup source mocks
        mock_source_obj = MagicMock()
        mock_source_func.return_value = mock_source_obj
        mock_filtered_source = MagicMock()
        mock_source_obj.with_resources.return_value = mock_filtered_source

        # Execute
        run_pipeline("all")

        # Verify pipeline init
        mock_pipeline.assert_called_once_with(
            pipeline_name="pubmed_abstracts",
            destination="postgres",
            dataset_name="pubmed",
            progress="log",
        )

        # Verify source.with_resources called
        mock_source_obj.with_resources.assert_called_once_with("pubmed_baseline", "pubmed_updates")

        # Verify run called with filtered source
        mock_p_instance.run.assert_called_once_with(mock_filtered_source)

        # Verify dbt called
        mock_run_dbt.assert_called_once()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_run_pipeline_updates_only(
        self,
        mock_run_dbt: MagicMock,
        mock_source_func: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test running only updates runs dbt."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance
        mock_info = MagicMock()
        mock_info.has_failed_jobs = False
        mock_p_instance.run.return_value = mock_info

        mock_source_obj = MagicMock()
        mock_source_func.return_value = mock_source_obj
        mock_filtered_source = MagicMock()
        mock_source_obj.with_resources.return_value = mock_filtered_source

        run_pipeline("updates")

        # Verify source.with_resources called
        mock_source_obj.with_resources.assert_called_once_with("pubmed_updates")

        # Verify run called with updates only
        mock_p_instance.run.assert_called_once_with(mock_filtered_source)

        # Verify dbt called
        mock_run_dbt.assert_called_once()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_dry_run(self, mock_dbt: MagicMock, mock_pipeline: MagicMock) -> None:
        """Test dry run skips execution."""
        run_pipeline("all", dry_run=True)

        mock_pipeline.assert_called_once()
        mock_pipeline.return_value.run.assert_not_called()
        mock_dbt.assert_not_called()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.sys.exit")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_failed_jobs_exit(
        self,
        mock_dbt: MagicMock,
        mock_source: MagicMock,
        mock_exit: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test that failed jobs trigger sys.exit(1)."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance

        mock_info = MagicMock()
        mock_info.has_failed_jobs = True
        mock_p_instance.run.return_value = mock_info

        run_pipeline("all")

        mock_exit.assert_called_once_with(1)

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    def test_run_pipeline_no_resources(self, mock_pipeline: MagicMock) -> None:
        """Test run_pipeline with invalid or empty target triggers warning and return."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance

        # 'none' or any string not in logic will result in empty resources list
        run_pipeline("invalid_target")

        # Should not run
        mock_p_instance.run.assert_not_called()

    @patch("coreason_etl_pubmedabstracts.main.get_args")
    @patch("coreason_etl_pubmedabstracts.main.run_pipeline")
    def test_main_success(self, mock_run: MagicMock, mock_args: MagicMock) -> None:
        """Test main() success path."""
        mock_args.return_value.load = "all"
        mock_args.return_value.dry_run = False

        main()

        mock_run.assert_called_once_with("all", False)

    @patch("coreason_etl_pubmedabstracts.main.get_args")
    @patch("coreason_etl_pubmedabstracts.main.run_pipeline")
    @patch("coreason_etl_pubmedabstracts.main.sys.exit")
    def test_main_exception(self, mock_exit: MagicMock, mock_run: MagicMock, mock_args: MagicMock) -> None:
        """Test main() exception handling."""
        mock_run.side_effect = Exception("Boom")

        main()

        mock_exit.assert_called_once_with(1)

    @patch("coreason_etl_pubmedabstracts.main.subprocess.run")
    def test_run_dbt_transformations_success(self, mock_subprocess: MagicMock) -> None:
        """Test run_dbt_transformations success."""
        run_dbt_transformations()

        mock_subprocess.assert_called_once_with(
            ["dbt", "build", "--project-dir", "dbt_pubmed"], check=True, capture_output=False
        )

    @patch("coreason_etl_pubmedabstracts.main.subprocess.run")
    def test_run_dbt_transformations_failure(self, mock_subprocess: MagicMock) -> None:
        """Test run_dbt_transformations failure."""
        mock_subprocess.side_effect = subprocess.CalledProcessError(1, ["dbt"])

        with self.assertRaises(subprocess.CalledProcessError):
            run_dbt_transformations()

    @patch("coreason_etl_pubmedabstracts.main.subprocess.run")
    def test_run_dbt_transformations_not_found(self, mock_subprocess: MagicMock) -> None:
        """Test run_dbt_transformations when dbt executable is missing."""
        mock_subprocess.side_effect = FileNotFoundError()

        with self.assertRaises(FileNotFoundError):
            run_dbt_transformations()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_fresh_baseline_truncates(
        self,
        mock_run_dbt: MagicMock,
        mock_source_func: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test that fresh run triggers TRUNCATE."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance
        mock_p_instance.dataset_name = "test_ds"

        # Mock success run
        mock_info = MagicMock()
        mock_info.has_failed_jobs = False
        mock_p_instance.run.return_value = mock_info

        # Mock empty state
        mock_p_instance.state = {}

        # Mock sql client
        mock_client = MagicMock()
        mock_p_instance.sql_client.return_value.__enter__.return_value = mock_client

        # Mock source
        mock_source_obj = MagicMock()
        mock_source_obj.name = "pubmed_source"  # Default name
        mock_source_func.return_value = mock_source_obj
        mock_filtered = MagicMock()
        mock_source_obj.with_resources.return_value = mock_filtered

        run_pipeline("baseline")

        # Verify TRUNCATE called
        mock_client.execute_sql.assert_called_with("TRUNCATE TABLE test_ds.bronze_pubmed_baseline")

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_resume_baseline_skips_truncate(
        self,
        mock_run_dbt: MagicMock,
        mock_source_func: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test that resuming run skips TRUNCATE."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance
        mock_p_instance.dataset_name = "test_ds"

        # Mock success run
        mock_info = MagicMock()
        mock_info.has_failed_jobs = False
        mock_p_instance.run.return_value = mock_info

        # Mock populated state
        mock_p_instance.state = {
            "sources": {
                "pubmed_source": {
                    "resources": {
                        "pubmed_baseline": {"incremental": {"file_name": {"last_value": "pubmed24n0001.xml.gz"}}}
                    }
                }
            }
        }

        mock_client = MagicMock()
        mock_p_instance.sql_client.return_value.__enter__.return_value = mock_client

        mock_source_obj = MagicMock()
        mock_source_obj.name = "pubmed_source"
        mock_source_func.return_value = mock_source_obj
        mock_filtered = MagicMock()
        mock_filtered.name = "pubmed_source"
        mock_source_obj.with_resources.return_value = mock_filtered

        run_pipeline("baseline")

        # Verify TRUNCATE NOT called
        mock_client.execute_sql.assert_not_called()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_baseline_truncate_exception(
        self,
        mock_run_dbt: MagicMock,
        mock_source_func: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test exception handling during truncate."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance
        mock_p_instance.dataset_name = "test_ds"

        # Mock success run
        mock_info = MagicMock()
        mock_info.has_failed_jobs = False
        mock_p_instance.run.return_value = mock_info

        # Fresh run
        mock_p_instance.state = {}

        # Mock client to raise exception
        mock_client = MagicMock()
        mock_p_instance.sql_client.return_value.__enter__.return_value = mock_client
        mock_client.execute_sql.side_effect = Exception("Table missing")

        # Setup source
        mock_source_obj = MagicMock()
        mock_source_obj.name = "pubmed_source"
        mock_source_func.return_value = mock_source_obj
        mock_filtered = MagicMock()
        mock_filtered.name = "pubmed_source"
        mock_source_obj.with_resources.return_value = mock_filtered

        # Should not raise
        run_pipeline("baseline")

        # Verify attempt
        mock_client.execute_sql.assert_called()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_baseline_state_access_failure(
        self,
        mock_run_dbt: MagicMock,
        mock_source_func: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test exception during state access is logged and ignored."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance

        # Mock successful run
        mock_info = MagicMock()
        mock_info.has_failed_jobs = False
        mock_p_instance.run.return_value = mock_info

        # Accessing state raises error
        type(mock_p_instance).state = PropertyMock(side_effect=Exception("State Corrupt"))

        mock_source_obj = MagicMock()
        mock_source_obj.name = "pubmed_source"
        mock_source_func.return_value = mock_source_obj
        mock_filtered = MagicMock()
        mock_filtered.name = "pubmed_source"
        mock_source_obj.with_resources.return_value = mock_filtered

        run_pipeline("baseline")

        # Should not crash
        mock_p_instance.run.assert_called()
