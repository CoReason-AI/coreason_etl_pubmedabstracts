# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import io
import unittest
from unittest.mock import MagicMock, call, patch

from coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline import pubmed_baseline
from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


class TestMoreComplexEdgeCases(unittest.TestCase):
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.open_remote_file")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.parse_pubmed_xml")
    def test_unsorted_ftp_listing_processed_sequentially(
        self, mock_parse: MagicMock, mock_open: MagicMock, mock_list: MagicMock
    ) -> None:
        """
        Verify that even if the FTP server returns files in random order,
        the pipeline processes them in alphanumeric order.
        This is critical for the 'incremental' logic (last_value) to work correctly.
        """
        # FTP returns files out of order
        mock_list.return_value = [
            "/pubmed/baseline/pubmed24n0003.xml.gz",
            "/pubmed/baseline/pubmed24n0001.xml.gz",
            "/pubmed/baseline/pubmed24n0002.xml.gz",
        ]

        # Setup mocks
        mock_open.return_value.__enter__.return_value = MagicMock()
        mock_parse.return_value = iter([])

        # Run pipeline
        resource = pubmed_baseline()
        list(resource)

        # Assertion: Verify that open_remote_file was called in SORTED order (0001, 0002, 0003)
        expected_calls = [
            call("ftp.ncbi.nlm.nih.gov", "/pubmed/baseline/pubmed24n0001.xml.gz"),
            call("ftp.ncbi.nlm.nih.gov", "/pubmed/baseline/pubmed24n0002.xml.gz"),
            call("ftp.ncbi.nlm.nih.gov", "/pubmed/baseline/pubmed24n0003.xml.gz"),
        ]

        # We compare call_args_list directly to avoid interference from child mock calls (context managers)
        self.assertEqual(mock_open.call_args_list, expected_calls)

    def test_complex_namespace_shadowing(self) -> None:
        """
        Verify that namespace stripping works even with shadowed or complex namespaces.
        """
        xml_content = (
            b'<PubmedArticleSet xmlns="http://default.ns" xmlns:ns2="http://other.ns">'
            b'    <ns2:MedlineCitation Status="MEDLINE" Owner="NLM">'
            b'        <ns2:PMID Version="1">12345</ns2:PMID>'
            b'        <Article PubModel="Print">'
            b"            <Journal>"
            b'                <ISSN IssnType="Print">0000-0000</ISSN>'
            b"            </Journal>"
            b'            <ArticleTitle xmlns="http://inner.ns">Title with <b xmlns="http://html.ns">Bold</b> '
            b"text</ArticleTitle>"
            b"        </Article>"
            b"    </ns2:MedlineCitation>"
            b"</PubmedArticleSet>"
        )

        stream = io.BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        doc = records[0]

        # Check stripping of keys
        self.assertIn("MedlineCitation", doc)
        # Check _record_type injection
        self.assertEqual(doc["_record_type"], "citation")
        # Check PMID extraction (no ns prefix)
        # FIXED: PMID is a list due to FORCE_LIST_KEYS
        self.assertEqual(doc["MedlineCitation"]["PMID"][0]["#text"], "12345")
        # Check ArticleTitle flattening (namespaces in title tag should be ignored/stripped)
        # The text is "Title with Bold text" (tags stripped)
        title = doc["MedlineCitation"]["Article"]["ArticleTitle"]
        self.assertEqual(title, "Title with Bold text")

    def test_broken_stream_during_parsing(self) -> None:
        """
        Verify that if the stream breaks (IOError) during iteration,
        the exception is raised and not swallowed.
        """

        class BrokenStream(io.BytesIO):
            def read(self, size: int | None = -1) -> bytes:
                raise IOError("Stream connection lost")

        stream = BrokenStream(b"<root>...")

        with self.assertRaises(IOError) as cm:
            list(parse_pubmed_xml(stream))
        self.assertEqual(str(cm.exception), "Stream connection lost")

    def test_malformed_delete_citation(self) -> None:
        """
        Verify parsing of a DeleteCitation that might be strangely formatted.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <DeleteCitation>
                <!-- Valid PMID -->
                <PMID>1001</PMID>
                <!-- Missing PMID? or empty -->
                <PMID></PMID>
            </DeleteCitation>
        </PubmedArticleSet>
        """
        stream = io.BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        doc = records[0]
        self.assertEqual(doc["_record_type"], "delete")

        # Check structure
        # DeleteCitation is in FORCE_LIST_KEYS, so it's a list.
        # PMID is also in FORCE_LIST_KEYS, so it's a list inside.
        pmids = doc["DeleteCitation"][0]["PMID"]
        self.assertIsInstance(pmids, list)
        self.assertEqual(len(pmids), 2)

        # First one is 1001
        # No attributes -> string "1001"
        self.assertEqual(pmids[0], "1001")

        # Second one is None? xmltodict behavior for empty tag <PMID></PMID> is None
        self.assertIsNone(pmids[1])

    def test_mixed_content_preservation_strict(self) -> None:
        """
        Strict test for mixed content flattening in AbstractText.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>999</PMID>
                <Article>
                    <Abstract>
                        <AbstractText>
                            Start <i>Italic</i> Middle <b>Bold</b> End <sub>Sub</sub>.
                        </AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = io.BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        abstract = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]
        # Expect tags stripped but whitespace/text preserved
        # "Start Italic Middle Bold End Sub."
        # Note: strip_tags removes the tag, keeping the text in place.
        # "Start " + "Italic" + " Middle " + "Bold" + " End " + "Sub" + "."
        expected = "Start Italic Middle Bold End Sub."

        # Normalize whitespace (replace newlines/tabs with space if any, though here it's clean)
        self.assertEqual(abstract.strip(), expected)

    def test_xml_with_comments_ignored(self) -> None:
        """
        Verify that XML comments are ignored during iteration and do not crash the pipeline.
        This hits the `if not isinstance(elem.tag, str): continue` line.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <!-- Top level comment -->
            <MedlineCitation Status="MEDLINE">
                <!-- Inner comment -->
                <PMID>888</PMID>
                <Article>
                    <ArticleTitle>Title with <!-- embedded comment --> Comment</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = io.BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        doc = records[0]
        # No attributes -> string "888"
        self.assertEqual(doc["MedlineCitation"]["PMID"][0], "888")
