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

from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


class TestAdditionalComplexCases(unittest.TestCase):
    def test_nested_force_list(self) -> None:
        """
        Test deeply nested structure where FORCE_LIST_KEYS applies.
        e.g. KeywordList -> Keyword
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>2001</PMID>
                <KeywordList Owner="NOTNLM">
                    <Keyword MajorTopicYN="N">gene therapy</Keyword>
                </KeywordList>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_record_type"], "citation")

        keywords = records[0]["MedlineCitation"]["KeywordList"]["Keyword"]
        self.assertIsInstance(keywords, list)
        self.assertEqual(len(keywords), 1)
        # With attributes, xmltodict returns a dict
        self.assertEqual(keywords[0]["#text"], "gene therapy")
        self.assertEqual(keywords[0]["@MajorTopicYN"], "N")

    def test_multiple_delete_citations(self) -> None:
        """
        Test parsing multiple DeleteCitation blocks in one file.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <DeleteCitation>
                <PMID>111</PMID>
                <PMID>222</PMID>
            </DeleteCitation>
            <MedlineCitation>
                <PMID>333</PMID>
            </MedlineCitation>
            <DeleteCitation>
                <PMID>444</PMID>
            </DeleteCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 3)

        self.assertEqual(records[0]["_record_type"], "delete")
        self.assertEqual(len(records[0]["DeleteCitation"][0]["PMID"]), 2)

        self.assertEqual(records[1]["_record_type"], "citation")
        # No attributes on PMID -> simplified to string by xmltodict
        self.assertEqual(records[1]["MedlineCitation"]["PMID"][0], "333")

        self.assertEqual(records[2]["_record_type"], "delete")
        self.assertEqual(len(records[2]["DeleteCitation"][0]["PMID"]), 1)
        self.assertEqual(records[2]["DeleteCitation"][0]["PMID"][0], "444")

    def test_delete_citation_with_attributes(self) -> None:
        """
        Test DeleteCitation with attributes (if any).
        Usually DeleteCitation doesn't have attributes, but let's test robustness.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <DeleteCitation SomeAttr="Value">
                <PMID>555</PMID>
            </DeleteCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_record_type"], "delete")

        delete_list = records[0]["DeleteCitation"]
        self.assertIsInstance(delete_list, list)
        self.assertEqual(len(delete_list), 1)
        self.assertEqual(delete_list[0]["@SomeAttr"], "Value")
        # No attributes on PMID -> simplified to string
        self.assertEqual(delete_list[0]["PMID"][0], "555")

    def test_complex_mixed_content(self) -> None:
        """
        Test a mix of citations and deletes with different structures.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID>1</PMID>
            </MedlineCitation>
            <DeleteCitation>
                <PMID>2</PMID>
            </DeleteCitation>
            <MedlineCitation Status="In-Process">
                <PMID>3</PMID>
                <Article>
                    <ArticleTitle>Title 3</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["_record_type"], "citation")
        self.assertEqual(records[1]["_record_type"], "delete")
        self.assertEqual(records[2]["_record_type"], "citation")

        self.assertEqual(records[2]["MedlineCitation"]["@Status"], "In-Process")
