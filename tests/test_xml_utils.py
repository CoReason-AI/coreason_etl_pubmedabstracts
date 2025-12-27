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
        # PMID should be a list now because of FORCE_LIST_KEYS
        self.assertIsInstance(records[0]["MedlineCitation"]["PMID"], list)
        self.assertEqual(records[0]["MedlineCitation"]["PMID"][0]["#text"], "123456")

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
        # DeleteCitation is in FORCE_LIST_KEYS, so it is a list
        self.assertIsInstance(records[0]["DeleteCitation"], list)
        delete_citation = records[0]["DeleteCitation"][0]

        # PMID is in FORCE_LIST_KEYS, so it is a list
        pmids = delete_citation["PMID"]
        self.assertIsInstance(pmids, list)
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

    def test_normalization_force_list(self) -> None:
        """Test that specified keys are always lists even if single."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>100</PMID>
                <Article>
                    <AuthorList>
                        <Author>
                            <LastName>Doe</LastName>
                            <ForeName>John</ForeName>
                        </Author>
                    </AuthorList>
                    <PublicationTypeList>
                        <PublicationType>Journal Article</PublicationType>
                    </PublicationTypeList>
                </Article>
                <MeshHeadingList>
                    <MeshHeading>
                        <DescriptorName>Science</DescriptorName>
                    </MeshHeading>
                </MeshHeadingList>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        citation = records[0]["MedlineCitation"]

        # Author should be a list
        authors = citation["Article"]["AuthorList"]["Author"]
        self.assertIsInstance(authors, list)
        self.assertEqual(len(authors), 1)
        self.assertEqual(authors[0]["LastName"], "Doe")

        # PublicationType should be a list
        pub_types = citation["Article"]["PublicationTypeList"]["PublicationType"]
        self.assertIsInstance(pub_types, list)
        self.assertEqual(len(pub_types), 1)
        # Without attributes, xmltodict returns the string directly
        self.assertEqual(pub_types[0], "Journal Article")

        # MeshHeading should be a list
        mesh_headings = citation["MeshHeadingList"]["MeshHeading"]
        self.assertIsInstance(mesh_headings, list)
        self.assertEqual(len(mesh_headings), 1)
