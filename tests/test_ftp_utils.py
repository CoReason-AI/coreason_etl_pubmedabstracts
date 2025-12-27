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
from coreason_etl_pubmedabstracts.pipelines.ftp_utils import list_remote_files


class TestFtpUtils(unittest.TestCase):
    @patch("coreason_etl_pubmedabstracts.pipelines.ftp_utils.fsspec")
    def test_list_remote_files(self, mock_fsspec: MagicMock) -> None:
        # Setup mock filesystem
        mock_fs = MagicMock()
        mock_fsspec.filesystem.return_value = mock_fs

        # Mock glob return
        mock_fs.glob.return_value = [
            "/pubmed/baseline/pubmed24n0002.xml.gz",
            "/pubmed/baseline/pubmed24n0001.xml.gz",
            "/pubmed/baseline/pubmed24n0003.xml.gz",
        ]

        # Call the function
        result = list_remote_files(
            host="ftp.ncbi.nlm.nih.gov",
            directory="/pubmed/baseline/"
        )

        # Verify fsspec was called correctly
        mock_fsspec.filesystem.assert_called_with(
            "ftp", host="ftp.ncbi.nlm.nih.gov", user="anonymous", password=""
        )

        mock_fs.glob.assert_called_with("/pubmed/baseline/*.xml.gz")

        # Verify result is sorted
        expected = [
            "/pubmed/baseline/pubmed24n0001.xml.gz",
            "/pubmed/baseline/pubmed24n0002.xml.gz",
            "/pubmed/baseline/pubmed24n0003.xml.gz",
        ]
        self.assertEqual(result, expected)

    @patch("coreason_etl_pubmedabstracts.pipelines.ftp_utils.fsspec")
    def test_list_remote_files_empty(self, mock_fsspec: MagicMock) -> None:
        mock_fs = MagicMock()
        mock_fsspec.filesystem.return_value = mock_fs
        mock_fs.glob.return_value = []

        result = list_remote_files("host", "dir")
        self.assertEqual(result, [])
