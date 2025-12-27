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

from dlt.extract.exceptions import ResourceExtractionError

from coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline import (
    pubmed_baseline,
    pubmed_updates,
)


class TestPubmedPipeline(unittest.TestCase):
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.open_remote_file")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.parse_pubmed_xml")
    def test_pubmed_baseline_configuration(
        self, mock_parse: MagicMock, mock_open: MagicMock, mock_list: MagicMock
    ) -> None:
        """Test that pubmed_baseline is configured correctly."""
        # Setup mocks
        mock_list.return_value = ["/pubmed/baseline/pubmed24n0001.xml.gz"]
        mock_open.return_value.__enter__.return_value = MagicMock()
        mock_parse.return_value = iter([{"MedlineCitation": {"PMID": "1"}}])

        # Inspect the resource object directly without iterating if possible,
        # but dlt resources are functions decorated.
        # We can check the .table_name attribute of the resource.

        resource = pubmed_baseline()
        self.assertEqual(resource.table_name, "bronze_pubmed_baseline")
        self.assertEqual(resource.write_disposition, "replace")

        # Iterate to verify logic flow
        records = list(resource)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["file_name"], "pubmed24n0001.xml.gz")
        self.assertEqual(records[0]["raw_data"]["MedlineCitation"]["PMID"], "1")

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.open_remote_file")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.parse_pubmed_xml")
    def test_pubmed_updates_configuration(
        self, mock_parse: MagicMock, mock_open: MagicMock, mock_list: MagicMock
    ) -> None:
        """Test that pubmed_updates is configured correctly."""
        mock_list.return_value = ["/pubmed/updatefiles/pubmed24n1001.xml.gz"]
        mock_open.return_value.__enter__.return_value = MagicMock()
        mock_parse.return_value = iter([{"MedlineCitation": {"PMID": "2"}}])

        resource = pubmed_updates()
        self.assertEqual(resource.table_name, "bronze_pubmed_updates")
        self.assertEqual(resource.write_disposition, "append")

        records = list(resource)
        self.assertEqual(len(records), 1)

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.open_remote_file")
    def test_file_open_error(self, mock_open: MagicMock, mock_list: MagicMock) -> None:
        """Test resilience when file opening fails."""
        mock_list.return_value = ["/pubmed/baseline/file1.xml.gz"]
        mock_open.side_effect = IOError("Network Error")

        resource = pubmed_baseline()

        # dlt wraps exceptions in ResourceExtractionError
        with self.assertRaises(ResourceExtractionError) as cm:
            list(resource)

        self.assertIn("Network Error", str(cm.exception))
