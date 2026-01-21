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


class TestMixedContentAndComplexity(unittest.TestCase):
    """
    Tests for complex XML structures and mixed content (HTML-like tags within text).
    """

    def test_mixed_content_article_title(self) -> None:
        """
        Test that ArticleTitle with internal tags (<i>, <b>, <sup>) is flattened to a single string.
        Without flattening, xmltodict creates a dictionary and splits the text, causing data loss.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>3001</PMID>
                <Article>
                    <ArticleTitle>The role of <i>Helicobacter pylori</i> in <b>gastritis</b>.</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]

        # We expect a simple string with the full text
        self.assertIsInstance(title, str)
        self.assertEqual(title, "The role of Helicobacter pylori in gastritis.")

    def test_mixed_content_abstract_text(self) -> None:
        """
        Test that AbstractText with mixed content is flattened.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>3002</PMID>
                <Article>
                    <Abstract>
                        <AbstractText>We observed <sub>decreased</sub> levels.</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        abstract_text = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]

        # Should be a list (standard behavior if force_list/multiple siblings, but here it's one sibling?)
        # Wait, FORCE_LIST_KEYS does not include AbstractText in the current code (I verified earlier).
        # So it might be a string or a list depending on sibling count.
        # However, mixed content makes it a DICT if not flattened.

        # If flattened, it should be a string (or list of strings if multiple AbstractText).
        # Here we have one AbstractText.

        # Note: If XML is <AbstractText>Text</AbstractText>, xmltodict -> 'Text'
        # If <AbstractText>A <b>B</b></AbstractText>, xmltodict -> {'#text': 'A ', 'b': 'B'} (Data Loss!)

        # Expectation after fix:
        self.assertEqual(abstract_text, "We observed decreased levels.")

    def test_complex_author_list(self) -> None:
        """
        Test AuthorList containing both PersonalName and CollectiveName.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>3003</PMID>
                <Article>
                    <AuthorList>
                        <Author>
                            <LastName>Smith</LastName>
                            <ForeName>John</ForeName>
                            <Initials>J</Initials>
                        </Author>
                        <Author>
                            <CollectiveName>The Big Study Group</CollectiveName>
                        </Author>
                    </AuthorList>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        authors = records[0]["MedlineCitation"]["Article"]["AuthorList"]["Author"]
        self.assertIsInstance(authors, list)
        self.assertEqual(len(authors), 2)

        self.assertEqual(authors[0]["LastName"], "Smith")
        self.assertEqual(authors[1]["CollectiveName"], "The Big Study Group")
        self.assertNotIn("LastName", authors[1])

    def test_grant_list_variance(self) -> None:
        """
        Test GrantList parsing.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>3004</PMID>
                <Article>
                    <GrantList>
                        <Grant>
                            <GrantID>R01 HL12345</GrantID>
                            <Acronym>HL</Acronym>
                            <Agency>NHLBI NIH HHS</Agency>
                            <Country>United States</Country>
                        </Grant>
                    </GrantList>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        grants = records[0]["MedlineCitation"]["Article"]["GrantList"]["Grant"]
        self.assertIsInstance(grants, list)
        self.assertEqual(grants[0]["GrantID"], "R01 HL12345")

    def test_chemical_list(self) -> None:
        """
        Test ChemicalList parsing.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>3005</PMID>
                <ChemicalList>
                    <Chemical>
                        <RegistryNumber>0</RegistryNumber>
                        <NameOfSubstance UI="D001234">Aspirine</NameOfSubstance>
                    </Chemical>
                </ChemicalList>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        chemicals = records[0]["MedlineCitation"]["ChemicalList"]["Chemical"]
        self.assertIsInstance(chemicals, list)
        # NameOfSubstance is forced as a list, so we must access index 0
        self.assertIsInstance(chemicals[0]["NameOfSubstance"], list)
        self.assertEqual(chemicals[0]["NameOfSubstance"][0]["#text"], "Aspirine")
        self.assertEqual(chemicals[0]["NameOfSubstance"][0]["@UI"], "D001234")
