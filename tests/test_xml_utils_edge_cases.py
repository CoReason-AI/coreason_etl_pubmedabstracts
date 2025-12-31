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


class TestXmlUtilsEdgeCases(unittest.TestCase):
    def test_truncated_xml_in_middle_of_tag(self) -> None:
        """
        Test that XMLSyntaxError is raised if the stream is truncated
        in the middle of a tag.
        """
        xml_content = b"<PubmedArticleSet><MedlineCi"
        stream = BytesIO(xml_content)

        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(stream))

    def test_cdata_handling(self) -> None:
        """
        Test that CDATA sections are parsed correctly as text.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <ArticleTitle><![CDATA[Some <nested> markup]]></ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        self.assertEqual(title, "Some <nested> markup")

    def test_namespace_prefix_preservation(self) -> None:
        """
        Verify that namespaces are preserved in keys, which is crucial
        because our logic might expect specific keys.
        """
        xml_content = b"""
        <ns:PubmedArticleSet xmlns:ns="http://example.com">
            <ns:MedlineCitation>
                <ns:PMID>123</ns:PMID>
            </ns:MedlineCitation>
        </ns:PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertIn("ns:MedlineCitation", records[0])
        self.assertEqual(records[0]["ns:MedlineCitation"]["ns:PMID"]["#text"], "123")
