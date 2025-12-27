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


class TestComplexScenarios(unittest.TestCase):
    def test_utf8_encoding(self) -> None:
        """
        Verify that non-ASCII characters are preserved correctly.
        """
        # "α-Helix" and "café"
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID>1001</PMID>
                <Article>
                    <ArticleTitle>Structure of \xce\xb1-Helix in caf\xc3\xa9</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        self.assertIn("\u03b1-Helix", title)
        self.assertIn("caf\u00e9", title)

    def test_structural_variance_list_vs_dict(self) -> None:
        """
        Verify that single items are forced to lists due to normalization.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID>1</PMID>
                <Article>
                    <AuthorList>
                        <Author><LastName>Doe</LastName></Author>
                    </AuthorList>
                </Article>
            </MedlineCitation>
            <MedlineCitation Status="MEDLINE">
                <PMID>2</PMID>
                <Article>
                    <AuthorList>
                        <Author><LastName>Smith</LastName></Author>
                        <Author><LastName>Jones</LastName></Author>
                    </AuthorList>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 2)

        # First record: Single Author -> Should be list now
        authors1 = records[0]["MedlineCitation"]["Article"]["AuthorList"]["Author"]
        self.assertIsInstance(authors1, list)
        self.assertEqual(len(authors1), 1)
        self.assertEqual(authors1[0]["LastName"], "Doe")

        # Second record: Multiple Authors -> List
        authors2 = records[1]["MedlineCitation"]["Article"]["AuthorList"]["Author"]
        self.assertIsInstance(authors2, list)
        self.assertEqual(len(authors2), 2)
        self.assertEqual(authors2[0]["LastName"], "Smith")
        self.assertEqual(authors2[1]["LastName"], "Jones")

    def test_mixed_elements_stream(self) -> None:
        """
        Verify streaming of interleaved DeleteCitation and MedlineCitation.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <DeleteCitation><PMID>99</PMID></DeleteCitation>
            <MedlineCitation Status="MEDLINE"><PMID>100</PMID></MedlineCitation>
            <DeleteCitation><PMID>98</PMID></DeleteCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 3)
        self.assertIn("DeleteCitation", records[0])
        self.assertIn("MedlineCitation", records[1])
        self.assertIn("DeleteCitation", records[2])

        # DeleteCitation is a list, and PMID is a list
        self.assertEqual(records[0]["DeleteCitation"][0]["PMID"][0], "99")
        self.assertEqual(records[1]["MedlineCitation"]["PMID"][0], "100")
        self.assertEqual(records[2]["DeleteCitation"][0]["PMID"][0], "98")
