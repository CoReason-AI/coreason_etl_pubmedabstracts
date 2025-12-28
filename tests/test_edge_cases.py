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

from lxml import etree

from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


class TestEdgeCases(unittest.TestCase):
    def test_malformed_xml(self) -> None:
        """
        Verify that a syntax error is raised for invalid XML.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1001</PMID>
            <!-- Missing closing tag for MedlineCitation -->
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)

        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(stream))

    def test_empty_xml_file(self) -> None:
        """
        Verify that an empty file (or one with just root) yields nothing.
        """
        xml_content = b"<PubmedArticleSet></PubmedArticleSet>"
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))
        self.assertEqual(len(records), 0)

    def test_empty_stream(self) -> None:
        """
        Verify that a 0-byte stream raises an XMLSyntaxError.
        """
        stream = BytesIO(b"")
        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(stream))

    def test_namespaces(self) -> None:
        """
        Verify handling of namespaces.

        We expect the parser to correctly identify the record type even with namespaces,
        and inject `_record_type`. Keys will still have prefixes as per xmltodict default.
        """
        xml_content = b"""
        <ns:PubmedArticleSet xmlns:ns="http://example.com/ns">
            <ns:MedlineCitation Status="MEDLINE">
                <ns:PMID>1001</ns:PMID>
            </ns:MedlineCitation>
        </ns:PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        record = records[0]

        # xmltodict preserves prefixes
        self.assertIn("ns:MedlineCitation", record)

        # Updated behavior: _record_type IS injected correctly
        self.assertEqual(record.get("_record_type"), "citation")

    def test_root_attributes_preservation(self) -> None:
        """
        Verify attributes on the root element of the chunk (MedlineCitation) are preserved.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE" Owner="NLM" VersionID="123">
                <PMID>1001</PMID>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_record_type"], "citation")
        citation = records[0]["MedlineCitation"]
        self.assertEqual(citation["@Status"], "MEDLINE")
        self.assertEqual(citation["@Owner"], "NLM")
        self.assertEqual(citation["@VersionID"], "123")

    def test_cdata_section(self) -> None:
        """
        Verify that CDATA is parsed as text.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1001</PMID>
                <Article>
                    <AbstractText><![CDATA[Here is some <b>bold</b> text & symbols]]></AbstractText>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_record_type"], "citation")
        abstract = records[0]["MedlineCitation"]["Article"]["AbstractText"]
        self.assertEqual(abstract, "Here is some <b>bold</b> text & symbols")

    def test_missing_medline_citation(self) -> None:
        """
        Verify behavior when the root tag is something unexpected.
        The parser filters for MedlineCitation/DeleteCitation.
        If they are not found, nothing is yielded.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <SomeOtherTag>
                <PMID>1001</PMID>
            </SomeOtherTag>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))
        self.assertEqual(len(records), 0)
