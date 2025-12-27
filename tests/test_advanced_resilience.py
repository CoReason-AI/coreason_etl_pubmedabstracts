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
from unittest.mock import MagicMock, patch

from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


class TestAdvancedResilience(unittest.TestCase):
    def test_deep_nesting(self) -> None:
        """
        Verify parsing of deeply nested elements where multiple levels are forced to lists.
        Hierarchy: MedlineCitation -> Article -> AuthorList -> Author -> AffiliationInfo -> Affiliation
        Keys forced to list: Author, Affiliation (via FORCE_LIST_KEYS? Need to check)
        Let's check xml_utils.py FORCE_LIST_KEYS.
        """
        # We assume FORCE_LIST_KEYS includes 'Author'.
        # Let's verify if 'Affiliation' is in there.
        # Checking logic: keys are explicit.
        # If 'Affiliation' is NOT in FORCE_LIST_KEYS, it will be a dict if single.
        # But 'Author' IS.

        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article>
                    <AuthorList>
                        <Author>
                            <LastName>Doe</LastName>
                            <AffiliationInfo>
                                <Affiliation>University of Test</Affiliation>
                            </AffiliationInfo>
                        </Author>
                    </AuthorList>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        # Author should be a list
        authors = records[0]["MedlineCitation"]["Article"]["AuthorList"]["Author"]
        self.assertIsInstance(authors, list)

        # AffiliationInfo usually contains Affiliation.
        # If Affiliation is not forced, it's a string/dict.
        # Let's inspect what we get.
        affil_info = authors[0]["AffiliationInfo"]
        # xmltodict behavior: <AffiliationInfo><Affiliation>...</Affiliation></AffiliationInfo>
        # affil_info['Affiliation'] -> "University of Test"
        self.assertEqual(affil_info["Affiliation"], "University of Test")

    def test_mixed_content(self) -> None:
        """
        Verify parsing of elements containing both text and child tags.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <GeneralNote>
                    This is a note with <b class="bold">bold</b> text.
                </GeneralNote>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        # GeneralNote is in FORCE_LIST_KEYS
        notes = records[0]["MedlineCitation"]["GeneralNote"]
        self.assertIsInstance(notes, list)

        # Mixed content in xmltodict usually results in:
        # {'#text': 'This is a note with ', 'b': {'@class': 'bold', '#text': 'bold'}, ...}
        # OR it might just be text if mixed=True is not default?
        # xmltodict default handles mixed content by splitting.
        # Let's verify the structure.
        note = notes[0]
        self.assertIn("#text", note)
        self.assertTrue(note["#text"].startswith("This is a note with"))
        self.assertIn("b", note)

    @patch("coreason_etl_pubmedabstracts.pipelines.xml_utils.etree.QName")
    @patch("coreason_etl_pubmedabstracts.pipelines.xml_utils.etree.iterparse")
    @patch("coreason_etl_pubmedabstracts.pipelines.xml_utils.etree.tostring")
    def test_memory_clearing(
        self, mock_tostring: MagicMock, mock_iterparse: MagicMock, mock_qname: MagicMock
    ) -> None:
        """
        Verify that elem.clear() and parent cleanup are called to prevent memory leaks.
        """
        # Setup the mock element
        mock_elem = MagicMock()
        mock_parent = MagicMock()

        # Mock QName to return "MedlineCitation"
        mock_qname.return_value.localname = "MedlineCitation"

        # Setup parent behavior
        # getparent() returns mock_parent
        mock_elem.getparent.return_value = mock_parent

        # Setup getprevious behavior for the while loop
        # First call: not None (trigger delete)
        # Second call: None (stop loop)
        mock_elem.getprevious.side_effect = ["sibling", None]

        # Setup mock_parent to simulate list behavior for deletion
        # del elem.getparent()[0]
        # mock_parent[0] access isn't strictly needed if we just mock __delitem__ or __getitem__?
        # Actually `del elem.getparent()[0]` calls `mock_parent.__delitem__(0)`

        # iterparse yields (event, elem)
        mock_iterparse.return_value = iter([("end", mock_elem)])

        # tostring returns a valid XML string so xmltodict.parse works
        mock_tostring.return_value = "<MedlineCitation><PMID>1</PMID></MedlineCitation>"

        stream = BytesIO(b"dummy")
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)

        # Verify clear() was called
        mock_elem.clear.assert_called_once()

        # Verify cleanup of previous siblings
        # We expected loop to run once
        mock_elem.getparent.assert_called()  # Called to check parent
        mock_parent.__delitem__.assert_called_with(0)

    def test_unicode_surrogates_and_edge_chars(self) -> None:
        """
        Test resilience against tricky unicode characters.
        """
        # Emoji and mathematical symbols
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article>
                    <ArticleTitle>Study on \xf0\x9f\x98\x80 (Grinning Face) &amp; \xe2\x88\x91 (Sum)</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        # Check that characters are preserved
        self.assertIn("😀", title)
        self.assertIn("∑", title)
