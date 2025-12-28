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


class TestComplexXmlCases(unittest.TestCase):
    def test_elocation_id_extraction(self) -> None:
        """
        Test extracting DOI from ELocationID.
        Verifies that ELocationID is always a list and contains attributes.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1001</PMID>
                <Article>
                    <ELocationID EIdType="doi" ValidYN="Y">10.1056/NEJMoa123456</ELocationID>
                    <ELocationID EIdType="pii">NEJMoa123456</ELocationID>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        # Verify _record_type
        self.assertEqual(records[0]["_record_type"], "citation")

        article = records[0]["MedlineCitation"]["Article"]

        # Verify ELocationID is a list (enforced by FORCE_LIST_KEYS)
        self.assertIsInstance(article["ELocationID"], list)
        self.assertEqual(len(article["ELocationID"]), 2)

        # Verify attribute access
        # xmltodict uses @AttributeName convention
        doi_entry = next(item for item in article["ELocationID"] if item.get("@EIdType") == "doi")
        self.assertEqual(doi_entry["#text"], "10.1056/NEJMoa123456")

        pii_entry = next(item for item in article["ELocationID"] if item.get("@EIdType") == "pii")
        self.assertEqual(pii_entry["#text"], "NEJMoa123456")

    def test_single_elocation_id(self) -> None:
        """Test that a single ELocationID is still wrapped in a list."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1002</PMID>
                <Article>
                    <ELocationID EIdType="doi">10.1234/5678</ELocationID>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(records[0]["_record_type"], "citation")
        elocation = records[0]["MedlineCitation"]["Article"]["ELocationID"]
        self.assertIsInstance(elocation, list)
        self.assertEqual(len(elocation), 1)
        self.assertEqual(elocation[0]["@EIdType"], "doi")
        self.assertEqual(elocation[0]["#text"], "10.1234/5678")

    def test_structured_abstract(self) -> None:
        """
        Test parsing of structured abstracts (with labels).
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1003</PMID>
                <Article>
                    <Abstract>
                        <AbstractText Label="BACKGROUND" NlmCategory="BACKGROUND">Here is the background.</AbstractText>
                        <AbstractText Label="METHODS" NlmCategory="METHODS">We did something.</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(records[0]["_record_type"], "citation")
        abstract_texts = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]

        # Note: AbstractText is NOT in FORCE_LIST_KEYS in the provided file.
        # xmltodict default behavior for multiple siblings is to create a list.
        self.assertIsInstance(abstract_texts, list)
        self.assertEqual(len(abstract_texts), 2)
        self.assertEqual(abstract_texts[0]["@Label"], "BACKGROUND")
        self.assertEqual(abstract_texts[0]["#text"], "Here is the background.")

    def test_utf8_handling(self) -> None:
        """Test parsing of UTF-8 characters."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1004</PMID>
                <Article>
                    <ArticleTitle>M\xc3\xa9n\xc3\xa8trier's Disease</ArticleTitle>
                    <AuthorList>
                        <Author>
                            <LastName>Nu\xc3\xb1ez</LastName>
                        </Author>
                    </AuthorList>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(records[0]["_record_type"], "citation")
        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        self.assertEqual(title, "Ménètrier's Disease")

        author_last_name = records[0]["MedlineCitation"]["Article"]["AuthorList"]["Author"][0]["LastName"]
        self.assertEqual(author_last_name, "Nuñez")

    def test_namespace_handling(self) -> None:
        """Test that namespaces don't break the parser."""
        xml_content = b"""
        <PubmedArticleSet xmlns="http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd">
            <MedlineCitation Status="MEDLINE">
                <PMID>1005</PMID>
                <Article>
                    <ArticleTitle>Namespace Test</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        # xmltodict generally handles default namespaces by ignoring them or handling them transparently
        # unless configured otherwise.
        self.assertEqual(len(records), 1)
        self.assertIn("MedlineCitation", records[0])
        self.assertEqual(records[0]["_record_type"], "citation")
        self.assertEqual(records[0]["MedlineCitation"]["Article"]["ArticleTitle"], "Namespace Test")

    def test_empty_elocation(self) -> None:
        """Test case where ELocationID tag exists but is empty."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1006</PMID>
                <Article>
                    <ELocationID EIdType="doi"/>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(records[0]["_record_type"], "citation")
        elocation = records[0]["MedlineCitation"]["Article"]["ELocationID"]
        self.assertIsInstance(elocation, list)
        self.assertEqual(elocation[0]["@EIdType"], "doi")
        # Empty tag usually results in None for #text or just attributes
        self.assertIsNone(elocation[0].get("#text"))
