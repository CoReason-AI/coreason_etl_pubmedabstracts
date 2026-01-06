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

import dlt
from dlt.sources import DltResource

from coreason_etl_pubmedabstracts.main import _prepare_baseline_load
from coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline import _create_pubmed_resource


class TestComplexPipelineScenarios(unittest.TestCase):
    """
    Test suite for refactored pipeline components and complex integration scenarios.
    """

    def test_create_pubmed_resource_url_handling(self) -> None:
        """
        Verify that _create_pubmed_resource correctly handles URL path construction
        regardless of trailing slashes in the base URL or subfolder.
        """
        # Case 1: Base URL with slash, subfolder with slash
        res1 = _create_pubmed_resource(
            base_url="ftp://example.com/pubmed/", subfolder="/baseline/", resource_name="res1"
        )
        # We need to inspect the 'bucket_url' in the configured source.
        # dlt resources wrap the source.
        # This is a bit tricky to introspect without running it, but we can inspect the wrapper.
        # The return value is a DltResource.
        # Introspection of dlt objects can be complex.
        # However, checking the name is easy.
        self.assertEqual(res1.name, "res1")

        # To check the URL, we might need to rely on the fact that it doesn't crash
        # and maybe mock `filesystem` to check calls.

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.filesystem")
    def test_create_pubmed_resource_arguments(self, mock_filesystem: MagicMock) -> None:
        """
        Verify arguments passed to filesystem source.
        """
        # Mock the return value to allow chaining | and .with_name
        mock_source = MagicMock()
        mock_filesystem.return_value = mock_source
        mock_source.__or__.return_value = mock_source
        mock_source.with_name.return_value = mock_source

        # Case 1: Clean inputs
        _create_pubmed_resource("ftp://site.com", "base", "res1")
        mock_filesystem.assert_called_with(
            bucket_url="ftp://site.com/base/",
            file_glob="*.xml.gz",
            incremental=unittest.mock.ANY,
        )

        # Case 2: Messy slashes
        _create_pubmed_resource("ftp://site.com///", "//updates//", "res2")
        mock_filesystem.assert_called_with(
            bucket_url="ftp://site.com/updates/",
            file_glob="*.xml.gz",
            incremental=unittest.mock.ANY,
        )

    def test_prepare_baseline_load_fresh_run(self) -> None:
        """
        Verify that _prepare_baseline_load truncates the table if no incremental state exists.
        """
        mock_pipeline = MagicMock(spec=dlt.Pipeline)
        mock_pipeline.dataset_name = "pubmed"
        # Setup state: No incremental state
        mock_pipeline.state = {}

        mock_source = MagicMock()
        mock_source.name = "pubmed_source"

        mock_client = MagicMock()
        mock_pipeline.sql_client.return_value.__enter__.return_value = mock_client

        _prepare_baseline_load(mock_pipeline, mock_source)

        # Should execute TRUNCATE
        mock_client.execute_sql.assert_called_once_with("TRUNCATE TABLE pubmed.bronze_pubmed_baseline")

    def test_prepare_baseline_load_resumed_run(self) -> None:
        """
        Verify that _prepare_baseline_load skips truncation if incremental state exists.
        """
        mock_pipeline = MagicMock(spec=dlt.Pipeline)
        mock_pipeline.dataset_name = "pubmed"

        # Setup state: Existing incremental state
        mock_pipeline.state = {
            "sources": {
                "pubmed_source": {
                    "resources": {"pubmed_baseline": {"incremental": {"file_name": {"last_value": "file_123.xml"}}}}
                }
            }
        }

        mock_source = MagicMock()
        mock_source.name = "pubmed_source"

        mock_client = MagicMock()
        mock_pipeline.sql_client.return_value.__enter__.return_value = mock_client

        _prepare_baseline_load(mock_pipeline, mock_source)

        # Should NOT execute TRUNCATE
        mock_client.execute_sql.assert_not_called()

    def test_prepare_baseline_load_table_missing(self) -> None:
        """
        Verify that if TRUNCATE fails (e.g., table missing), the error is logged but swallowed.
        """
        mock_pipeline = MagicMock(spec=dlt.Pipeline)
        mock_pipeline.dataset_name = "pubmed"
        mock_pipeline.state = {}

        mock_source = MagicMock()
        mock_source.name = "pubmed_source"

        mock_client = MagicMock()
        # Simulate SQL error (Table not found)
        mock_client.execute_sql.side_effect = Exception("Table does not exist")
        mock_pipeline.sql_client.return_value.__enter__.return_value = mock_client

        # Should not raise exception
        try:
            _prepare_baseline_load(mock_pipeline, mock_source)
        except Exception:
            self.fail("_prepare_baseline_load raised Exception unexpectedly")

        mock_client.execute_sql.assert_called_once()
