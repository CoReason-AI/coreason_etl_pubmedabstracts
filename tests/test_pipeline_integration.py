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
from unittest.mock import MagicMock, patch

from coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline import pubmed_baseline, pubmed_source, pubmed_updates


class TestPipelineIntegration(unittest.TestCase):
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.open_remote_file")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    def test_pubmed_baseline_flow(self, mock_list: MagicMock, mock_open: MagicMock) -> None:
        """
        Test the flow from listing files to parsing and yielding.
        """
        # Mock listing
        mock_list.return_value = ["/pubmed/baseline/file1.xml.gz"]

        # Mock file content
        # Adding Version attribute to PMID to ensure xmltodict parses it as a dict with #text
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID Version="1">1001</PMID>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        # mock_open returns a context manager that yields the file object
        mock_file = BytesIO(xml_content)
        mock_open.return_value.__enter__.return_value = mock_file

        # Run the resource
        resource = pubmed_baseline(host="test_host")
        data = list(resource)

        # Verify
        mock_list.assert_called_with("test_host", "/pubmed/baseline/")
        mock_open.assert_called_with("test_host", "/pubmed/baseline/file1.xml.gz")

        self.assertEqual(len(data), 1)
        # Verify wrapped structure
        self.assertIn("raw_data", data[0])
        self.assertIn("file_name", data[0])
        self.assertIn("ingestion_ts", data[0])
        self.assertIn("content_hash", data[0])
        self.assertEqual(data[0]["file_name"], "file1.xml.gz")

        # Verify parsed content
        raw_data = data[0]["raw_data"]
        self.assertIn("MedlineCitation", raw_data)
        # PMID is a list now
        self.assertEqual(raw_data["MedlineCitation"]["PMID"][0]["#text"], "1001")

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.open_remote_file")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    def test_pubmed_updates_flow(self, mock_list: MagicMock, mock_open: MagicMock) -> None:
        """
        Test the flow for updates resource.
        """
        mock_list.return_value = ["/pubmed/updatefiles/update1.xml.gz"]

        xml_content = b"""
        <PubmedArticleSet>
            <DeleteCitation>
                <PMID Version="1">9999</PMID>
            </DeleteCitation>
        </PubmedArticleSet>
        """
        mock_file = BytesIO(xml_content)
        mock_open.return_value.__enter__.return_value = mock_file

        resource = pubmed_updates(host="test_host")
        data = list(resource)

        mock_list.assert_called_with("test_host", "/pubmed/updatefiles/")
        mock_open.assert_called_with("test_host", "/pubmed/updatefiles/update1.xml.gz")

        self.assertEqual(len(data), 1)
        self.assertIn("raw_data", data[0])
        self.assertIn("DeleteCitation", data[0]["raw_data"])

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    def test_source_yields_resources(self, mock_list: MagicMock) -> None:
        """
        Verify that the source contains both expected resources.
        We mock list_remote_files to prevent actual FTP calls when resources are instantiated.
        """
        mock_list.return_value = []

        source = pubmed_source()

        resource_names = list(source.resources.keys())
        # Both resources write to same table but have different names internally (function names)
        # dlt names resources by function name by default
        self.assertIn("pubmed_baseline", resource_names)
        self.assertIn("pubmed_updates", resource_names)
