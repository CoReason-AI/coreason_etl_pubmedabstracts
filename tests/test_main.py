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
from unittest.mock import MagicMock, PropertyMock, patch

from typer.testing import CliRunner

from coreason_etl_pubmedabstracts.main import app, main, run_dbt_transformations


class TestMainOrchestration(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_run_pipeline_all(
        self,
        mock_run_dbt: MagicMock,
        mock_source_func: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test running all resources triggers dbt via Typer command."""
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

        # Execute via Typer Runner
        result = self.runner.invoke(app, ["run", "--load", "all"])
        self.assertEqual(result.exit_code, 0)

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

        # Verify dbt called with pipeline instance
        mock_run_dbt.assert_called_once_with(mock_p_instance)

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

        result = self.runner.invoke(app, ["run", "--load", "updates"])
        self.assertEqual(result.exit_code, 0)

        # Verify source.with_resources called
        mock_source_obj.with_resources.assert_called_once_with("pubmed_updates")

        # Verify run called with updates only
        mock_p_instance.run.assert_called_once_with(mock_filtered_source)

        # Verify dbt called
        mock_run_dbt.assert_called_once_with(mock_p_instance)

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_dry_run(self, mock_dbt: MagicMock, mock_pipeline: MagicMock) -> None:
        """Test dry run skips execution."""
        result = self.runner.invoke(app, ["run", "--dry-run"])
        self.assertEqual(result.exit_code, 0)

        mock_pipeline.assert_called_once()
        mock_pipeline.return_value.run.assert_not_called()
        mock_dbt.assert_not_called()

    @patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
    @patch("coreason_etl_pubmedabstracts.main.pubmed_source")
    @patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
    def test_failed_jobs_exit(
        self,
        mock_dbt: MagicMock,
        mock_source: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test that failed jobs trigger sys.exit(1)."""
        mock_p_instance = MagicMock()
        mock_pipeline.return_value = mock_p_instance

        mock_info = MagicMock()
        mock_info.has_failed_jobs = True
        mock_p_instance.run.return_value = mock_info

        result = self.runner.invoke(app, ["run", "--load", "all"])
        # Typer catches sys.exit(1) and sets exit_code to 1
        self.assertEqual(result.exit_code, 1)

    @patch("coreason_etl_pubmedabstracts.main.app")
    def test_main_entrypoint(self, mock_app: MagicMock) -> None:
        """Test main() calls app()."""
        main()
        mock_app.assert_called_once()

    @patch("coreason_etl_pubmedabstracts.main.create_runner")
    def test_run_dbt_transformations_success(self, mock_create_runner: MagicMock) -> None:
        """Test run_dbt_transformations success with dlt runner."""
        mock_pipeline = MagicMock()
        mock_client = MagicMock()
        mock_pipeline.destination_client.return_value.__enter__.return_value = mock_client
        mock_client.config = "mock_config"

        mock_runner_instance = MagicMock()
        mock_create_runner.return_value = mock_runner_instance

        run_dbt_transformations(mock_pipeline)

        # Verify create_runner call
        mock_create_runner.assert_called_once_with(
            venv=None,
            credentials="mock_config",
            working_dir=".",
            package_location="dbt_pubmed",
        )

        # Verify execution of 'dbt build'
        mock_runner_instance._run_dbt_command.assert_called_once_with("build", cmd_params=["--fail-fast"])

    @patch("coreason_etl_pubmedabstracts.main.create_runner")
    def test_run_dbt_transformations_failure(self, mock_create_runner: MagicMock) -> None:
        """Test run_dbt_transformations failure handling."""
        mock_pipeline = MagicMock()
        mock_client = MagicMock()
        mock_pipeline.destination_client.return_value.__enter__.return_value = mock_client

        mock_runner_instance = MagicMock()
        mock_create_runner.return_value = mock_runner_instance

        # Simulate failure in _run_dbt_command
        mock_runner_instance._run_dbt_command.side_effect = Exception("DBT Failed")

        with self.assertRaisesRegex(Exception, "DBT Failed"):
            run_dbt_transformations(mock_pipeline)

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

        self.runner.invoke(app, ["--load", "baseline"])

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

        self.runner.invoke(app, ["--load", "baseline"])

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

        # Should not raise (log only)
        result = self.runner.invoke(app, ["--load", "baseline"])
        self.assertEqual(result.exit_code, 0)

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

        result = self.runner.invoke(app, ["--load", "baseline"])
        self.assertEqual(result.exit_code, 0)

        # Should not crash
        mock_p_instance.run.assert_called()
