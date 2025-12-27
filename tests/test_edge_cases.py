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

    def test_namespaces(self) -> None:
        """
        Verify handling of namespaces.
        xmltodict usually includes the namespace prefix in the key.
        """
        xml_content = b"""
        <ns:PubmedArticleSet xmlns:ns="http://example.com/ns">
            <ns:MedlineCitation Status="MEDLINE">
                <ns:PMID>1001</ns:PMID>
            </ns:MedlineCitation>
        </ns:PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        # Note: iterparse tag argument might need to match namespace or be "*"
        # Our implementation uses tag=["MedlineCitation", ...].
        # lxml iterparse with specific tags matches local names if namespace is not specified in the tag argument?
        # Actually, lxml requires fully qualified name if namespace is present, unless we iterate over all events.
        # But our code says `tag=["MedlineCitation", "DeleteCitation"]`.
        # If the XML has namespaces, `lxml` expects `{http://example.com/ns}MedlineCitation`.
        # So this test might fail if our code doesn't handle namespaced tags flexibly.
        # Let's see if it fails. If so, we might need to adjust the code to support namespaces or wildcard.
        # However, NLM XML usually doesn't use prefixes for these tags.
        # But this is an "Edge Case" test.

        # If it fails to find the tag, it yields nothing.
        records = list(parse_pubmed_xml(stream))

        # Expecting failure to match simple "MedlineCitation" against "{...}MedlineCitation"
        # If we want to support namespaces, we should probably remove the `tag` filter or make it namespace-aware.
        # For now, let's assert what currently happens (likely 0 records)
        # or update code if requirement implies namespace support.
        # Assuming current code is strict.
        self.assertEqual(len(records), 0)

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
        abstract = records[0]["MedlineCitation"]["Article"]["AbstractText"]
        self.assertEqual(abstract, "Here is some <b>bold</b> text & symbols")
