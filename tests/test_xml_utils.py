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


class TestXmlUtils(unittest.TestCase):
    def test_parse_medline_citation(self) -> None:
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE" Owner="NLM">
                <PMID Version="1">123456</PMID>
                <Article PubModel="Print">
                    <Journal>
                        <ISSN IssnType="Print">0028-4793</ISSN>
                        <Title>New England Journal of Medicine</Title>
                    </Journal>
                    <ArticleTitle>Test Article</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        self.assertIn("MedlineCitation", records[0])
        self.assertEqual(records[0]["MedlineCitation"]["PMID"]["#text"], "123456")

    def test_parse_delete_citation(self) -> None:
        xml_content = b"""
        <PubmedArticleSet>
            <DeleteCitation>
                <PMID Version="1">999999</PMID>
                <PMID Version="1">888888</PMID>
            </DeleteCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        self.assertIn("DeleteCitation", records[0])
        # xmltodict handles list of children with same name as list
        pmids = records[0]["DeleteCitation"]["PMID"]
        self.assertEqual(len(pmids), 2)
        self.assertEqual(pmids[0]["#text"], "999999")

    def test_parse_mixed(self) -> None:
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID>1</PMID>
            </MedlineCitation>
            <DeleteCitation>
                <PMID>2</PMID>
            </DeleteCitation>
            <MedlineCitation Status="MEDLINE">
                <PMID>3</PMID>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 3)
        self.assertIn("MedlineCitation", records[0])
        self.assertIn("DeleteCitation", records[1])
        self.assertIn("MedlineCitation", records[2])
