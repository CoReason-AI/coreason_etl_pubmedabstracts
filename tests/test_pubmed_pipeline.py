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

from coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline import (
    pubmed_source,
    pubmed_xml_parser,
)


class TestPubmedPipeline(unittest.TestCase):
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.filesystem")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.dlt.config.get")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.pubmed_xml_parser")
    def test_pubmed_source_configuration(
        self, mock_parser: MagicMock, mock_get: MagicMock, mock_filesystem: MagicMock
    ) -> None:
        """Test that pubmed_source configures filesystem resources correctly."""
        # Setup mock config
        mock_get.return_value = "ftp://mock_host/pubmed/"

        # Setup distinct mock resources for filesystem calls
        mock_fs_base = MagicMock()
        mock_fs_updates = MagicMock()
        mock_filesystem.side_effect = [mock_fs_base, mock_fs_updates]

        # Setup pipe results
        mock_pipe_base = MagicMock()
        mock_fs_base.__or__.return_value = mock_pipe_base

        # Use real DltResource objects for final return to satisfy dlt.source checks
        # These act as the result of .with_name(...)
        real_resource_baseline = dlt.resource([], name="pubmed_baseline")
        real_resource_updates = dlt.resource([], name="pubmed_updates")

        mock_pipe_base.with_name.return_value = real_resource_baseline

        mock_pipe_updates = MagicMock()
        mock_fs_updates.__or__.return_value = mock_pipe_updates
        mock_pipe_updates.with_name.return_value = real_resource_updates

        # Invoke
        source = pubmed_source()
        # dlt.source returns a DltSource object which contains resources
        resources = list(source.resources.values())

        # Verify
        self.assertEqual(len(resources), 2)
        resource_names = {r.name for r in resources}
        self.assertEqual(resource_names, {"pubmed_baseline", "pubmed_updates"})

        # Verify filesystem calls
        # 1. Baseline
        mock_filesystem.assert_any_call(
            bucket_url="ftp://mock_host/pubmed/baseline/",
            file_glob="*.xml.gz",
            incremental=unittest.mock.ANY,
        )
        # 2. Updates
        mock_filesystem.assert_any_call(
            bucket_url="ftp://mock_host/pubmed/updatefiles/",
            file_glob="*.xml.gz",
            incremental=unittest.mock.ANY,
        )

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.parse_pubmed_xml")
    def test_pubmed_xml_parser(self, mock_parse: MagicMock) -> None:
        """Test the transformer parses file items."""
        # Setup inputs
        # Use a plain mock to allow 'open' method
        mock_file_item = MagicMock()
        mock_file_item.__getitem__.side_effect = lambda k: "test_file.xml.gz" if k == "file_name" else None

        # Mock open context manager
        mock_file_handle = MagicMock()
        mock_file_item.open.return_value.__enter__.return_value = mock_file_handle

        # Mock parser output
        mock_parse.return_value = iter([{"MedlineCitation": {"PMID": "123"}}, {"MedlineCitation": {"PMID": "456"}}])

        source_data = [[mock_file_item]]
        source = dlt.resource(source_data, name="dummy_source")

        pipeline_step = source | pubmed_xml_parser

        results = list(pipeline_step)

        # Verify results
        self.assertEqual(len(results), 2)

        self.assertEqual(results[0]["file_name"], "test_file.xml.gz")
        self.assertEqual(results[0]["raw_data"]["MedlineCitation"]["PMID"], "123")

        mock_file_item.open.assert_called_once()
        mock_parse.assert_called_once_with(mock_file_handle)

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.parse_pubmed_xml")
    def test_pubmed_xml_parser_error_handling(self, mock_parse: MagicMock) -> None:
        """Test that parser errors are raised."""
        mock_file_item = MagicMock()
        mock_file_item.__getitem__.return_value = "bad_file.xml"
        mock_file_item.open.side_effect = Exception("Read Error")

        source_data = [[mock_file_item]]
        source = dlt.resource(source_data, name="dummy_source")

        pipeline_step = source | pubmed_xml_parser

        with self.assertRaises(Exception) as cm:
            list(pipeline_step)

        self.assertIn("Read Error", str(cm.exception))
