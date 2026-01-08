# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from coreason_etl_pubmedabstracts.main import app, main


@pytest.fixture  # type: ignore[misc]
def runner() -> CliRunner:
    return CliRunner()


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
@patch("coreason_etl_pubmedabstracts.main.pubmed_source")
@patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
def test_main_default_args(
    mock_dbt: MagicMock,
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
    runner: CliRunner,
) -> None:
    """Test running with default arguments (which defaults to 'all')."""
    # Setup mocks
    pipeline_instance = MagicMock()
    pipeline_instance.run.return_value.has_failed_jobs = False
    mock_pipeline.return_value = pipeline_instance

    source_instance = MagicMock()
    mock_source.return_value = source_instance
    source_instance.with_resources.return_value = source_instance

    config_mock = MagicMock()
    config_mock.__section__ = "postgres"
    pipeline_instance.destination_client.return_value.__enter__.return_value.config = config_mock

    result = runner.invoke(app, [])

    assert result.exit_code == 0, f"Exit code {result.exit_code}, Output: {result.output}"
    mock_pipeline.assert_called_once()
    mock_source.assert_called_once()
    source_instance.with_resources.assert_called_with("pubmed_baseline", "pubmed_updates")


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
@patch("coreason_etl_pubmedabstracts.main.pubmed_source")
@patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
def test_main_run_all(
    mock_dbt: MagicMock,
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
    runner: CliRunner,
) -> None:
    """Test successful run with default 'all'."""
    # Setup mocks
    pipeline_instance = MagicMock()
    pipeline_instance.run.return_value.has_failed_jobs = False
    mock_pipeline.return_value = pipeline_instance

    source_instance = MagicMock()
    mock_source.return_value = source_instance
    source_instance.with_resources.return_value = source_instance

    result = runner.invoke(app, ["--load", "all"])

    assert result.exit_code == 0, f"Exit code {result.exit_code}, Output: {result.output}"
    mock_pipeline.assert_called_once()
    mock_source.assert_called_once()
    source_instance.with_resources.assert_called_with("pubmed_baseline", "pubmed_updates")
    pipeline_instance.run.assert_called_once()
    mock_dbt.assert_called_once()


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
def test_main_dry_run(mock_pipeline: MagicMock, runner: CliRunner) -> None:
    """Test dry run flag."""
    result = runner.invoke(app, ["--dry-run"])

    assert result.exit_code == 0, f"Exit code {result.exit_code}, Output: {result.output}"
    mock_pipeline.assert_called_once()
    mock_pipeline.return_value.run.assert_not_called()


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
@patch("coreason_etl_pubmedabstracts.main.pubmed_source")
@patch("coreason_etl_pubmedabstracts.main._prepare_baseline_load")
def test_main_baseline_only(
    mock_prepare: MagicMock,
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
    runner: CliRunner,
) -> None:
    """Test running only baseline."""
    pipeline_instance = MagicMock()
    pipeline_instance.run.return_value.has_failed_jobs = False
    mock_pipeline.return_value = pipeline_instance

    source_instance = MagicMock()
    mock_source.return_value = source_instance
    source_instance.with_resources.return_value = source_instance

    config_mock = MagicMock()
    config_mock.__section__ = "postgres"
    pipeline_instance.destination_client.return_value.__enter__.return_value.config = config_mock

    with patch("coreason_etl_pubmedabstracts.main.create_runner") as mock_runner:
        mock_runner_instance = MagicMock()
        mock_runner.return_value = mock_runner_instance

        result = runner.invoke(app, ["--load", "baseline"])

        assert result.exit_code == 0, f"Exit code {result.exit_code}, Output: {result.output}"
        source_instance.with_resources.assert_called_with("pubmed_baseline")
        mock_prepare.assert_called_once()
        mock_runner_instance._run_dbt_command.assert_called_with("build", command_args=["--fail-fast"])


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
@patch("coreason_etl_pubmedabstracts.main.pubmed_source")
@patch("coreason_etl_pubmedabstracts.main._prepare_baseline_load")
def test_main_updates_only(
    mock_prepare: MagicMock,
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
    runner: CliRunner,
) -> None:
    """Test running only updates."""
    pipeline_instance = MagicMock()
    pipeline_instance.run.return_value.has_failed_jobs = False
    mock_pipeline.return_value = pipeline_instance

    source_instance = MagicMock()
    mock_source.return_value = source_instance
    source_instance.with_resources.return_value = source_instance

    config_mock = MagicMock()
    config_mock.__section__ = "postgres"
    pipeline_instance.destination_client.return_value.__enter__.return_value.config = config_mock

    with patch("coreason_etl_pubmedabstracts.main.create_runner") as mock_runner:
        mock_runner_instance = MagicMock()
        mock_runner.return_value = mock_runner_instance

        result = runner.invoke(app, ["--load", "updates"])

        assert result.exit_code == 0, f"Exit code {result.exit_code}, Output: {result.output}"
        source_instance.with_resources.assert_called_with("pubmed_updates")
        mock_prepare.assert_not_called()
        mock_runner_instance._run_dbt_command.assert_called_with("build", command_args=["--fail-fast"])


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
@patch("coreason_etl_pubmedabstracts.main.pubmed_source")
def test_pipeline_failure(
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
    runner: CliRunner,
) -> None:
    """Test handling of pipeline failure."""
    pipeline_instance = MagicMock()
    # Ensure this raises an Exception during run()
    pipeline_instance.run.side_effect = Exception("Boom")
    mock_pipeline.return_value = pipeline_instance

    # Mock source to ensure it proceeds to run()
    source_instance = MagicMock()
    mock_source.return_value = source_instance
    source_instance.with_resources.return_value = source_instance

    # We invoke it. We expect exit code 1.
    result = runner.invoke(app, [])

    assert result.exit_code == 1, f"Exit code {result.exit_code}, Output: {result.output}"
    mock_pipeline.assert_called_once()
    pipeline_instance.run.assert_called_once()


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
@patch("coreason_etl_pubmedabstracts.main.pubmed_source")
def test_failed_jobs_check(
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
    runner: CliRunner,
) -> None:
    """Test exit when dlt reports failed jobs."""
    pipeline_instance = MagicMock()
    pipeline_instance.run.return_value.has_failed_jobs = True
    mock_pipeline.return_value = pipeline_instance
    mock_source.return_value.with_resources.return_value = MagicMock()

    result = runner.invoke(app, [])

    assert result.exit_code == 1, f"Exit code {result.exit_code}, Output: {result.output}"


def test_script_entry_point() -> None:
    """Test the main() function entry point."""
    with patch("coreason_etl_pubmedabstracts.main.app") as mock_app:
        main()
        mock_app.assert_called_once()


def test_script_entry_point_exception() -> None:
    """Test exception handling in main()."""
    with patch("coreason_etl_pubmedabstracts.main.app") as mock_app:
        mock_app.side_effect = Exception("Crash")
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1


# --- Coverage gap filling tests ---


@patch("coreason_etl_pubmedabstracts.main.create_runner")
def test_run_dbt_transformations_failure(mock_create_runner: MagicMock) -> None:
    """Test run_dbt_transformations raises exception on failure."""
    from coreason_etl_pubmedabstracts.main import run_dbt_transformations

    mock_pipeline = MagicMock()
    # Mock context manager
    mock_client = MagicMock()
    mock_pipeline.destination_client.return_value.__enter__.return_value = mock_client
    mock_client.config.__section__ = "postgres"

    mock_create_runner.side_effect = Exception("DBT Failed")

    with pytest.raises(Exception, match="DBT Failed"):
        run_dbt_transformations(mock_pipeline)


def test_prepare_baseline_load_exception() -> None:
    """Test _prepare_baseline_load handles exceptions gracefully (logs warning)."""
    from coreason_etl_pubmedabstracts.main import _prepare_baseline_load

    mock_pipeline = MagicMock()
    # Force an attribute error to simulate crash accessing state
    del mock_pipeline.state

    mock_source = MagicMock()
    mock_source.name = "test_source"

    # Should not raise
    _prepare_baseline_load(mock_pipeline, mock_source)


def test_prepare_baseline_load_truncation_failure() -> None:
    """Test truncation failure logic inside _prepare_baseline_load."""
    from coreason_etl_pubmedabstracts.main import _prepare_baseline_load

    mock_pipeline = MagicMock()
    # Mock state to simulate fresh run (no last_value)
    mock_pipeline.state = {
        "sources": {
            "test_source": {
                "resources": {
                    "pubmed_baseline": {
                        "incremental": {
                            "file_name": {}  # Empty means fresh run
                        }
                    }
                }
            }
        }
    }

    mock_source = MagicMock()
    mock_source.name = "test_source"

    # Mock sql_client
    mock_sql_client = MagicMock()
    mock_pipeline.sql_client.return_value.__enter__.return_value = mock_sql_client
    # Simulate execute_sql failure
    mock_sql_client.execute_sql.side_effect = Exception("Table not found")

    # Should not raise, just log warning
    _prepare_baseline_load(mock_pipeline, mock_source)
    mock_sql_client.execute_sql.assert_called_once()


def test_prepare_baseline_load_truncation_success() -> None:
    """Test successful truncation logic inside _prepare_baseline_load."""
    from coreason_etl_pubmedabstracts.main import _prepare_baseline_load

    mock_pipeline = MagicMock()
    # Mock state to simulate fresh run (no last_value)
    mock_pipeline.state = {
        "sources": {
            "test_source": {
                "resources": {
                    "pubmed_baseline": {
                        "incremental": {
                            "file_name": {}  # Empty means fresh run
                        }
                    }
                }
            }
        }
    }

    mock_source = MagicMock()
    mock_source.name = "test_source"

    # Mock sql_client
    mock_sql_client = MagicMock()
    mock_pipeline.sql_client.return_value.__enter__.return_value = mock_sql_client

    # Should not raise
    _prepare_baseline_load(mock_pipeline, mock_source)
    mock_sql_client.execute_sql.assert_called_once()


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
@patch("coreason_etl_pubmedabstracts.main.pubmed_source")
@patch("coreason_etl_pubmedabstracts.main.run_dbt_transformations")
def test_run_with_no_resources(
    mock_dbt: MagicMock,
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
    runner: CliRunner,
) -> None:
    """Test run handles no resources selected (e.g. if we add filtering in future, or just defensive check)."""
    # Currently unreachable via CLI logic, but good to keep if logic changes.
    pass
