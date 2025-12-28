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

class TestComplexXmlCases(unittest.TestCase):
    def test_empty_stream(self) -> None:
        """Test handling of an empty stream."""
        # An empty stream isn't valid XML, so iterparse might raise an error immediately
        # or just yield nothing if the file is truly 0 bytes but iterparse expects a root.
        # Actually lxml iterparse raises XMLSyntaxError on empty input usually.
        stream = BytesIO(b"")
        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(stream))

    def test_malformed_xml(self) -> None:
        """Test handling of malformed XML."""
        xml_content = b"<PubmedArticleSet><MedlineCitation>Unclosed Tag</PubmedArticleSet>"
        stream = BytesIO(xml_content)
        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(stream))

    def test_cdata_handling(self) -> None:
        """Test that CDATA sections are parsed correctly as text."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <ArticleTitle><![CDATA[Study of <unknown> tags]]></ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        self.assertEqual(title, "Study of <unknown> tags")

    def test_attributes_only(self) -> None:
        """Test elements that have attributes but no text content."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article>
                    <AuthorList>
                        <Author ValidYN="Y" />
                    </AuthorList>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        author = records[0]["MedlineCitation"]["Article"]["AuthorList"]["Author"][0]
        # xmltodict behavior for empty tag with attributes: {'@ValidYN': 'Y', '#text': None} or similar?
        # Actually usually checks if #text is present. If self-closing <Tag Attr="Val" />, it might be just {'@Attr': 'Val'}.
        self.assertEqual(author["@ValidYN"], "Y")
        self.assertIsNone(author.get("#text"))

    def test_empty_self_closing_tags(self) -> None:
        """Test elements that are empty and self-closing."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article>
                    <Abstract>
                        <AbstractText/>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        abstract_text = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]
        # xmltodict usually returns None for <Tag/>
        self.assertIsNone(abstract_text)

    def test_default_namespace(self) -> None:
        """Test XML with a default namespace declaration."""
        xml_content = b"""
        <PubmedArticleSet xmlns="http://ncbi.nlm.nih.gov/pubmed">
            <MedlineCitation>
                <PMID>123</PMID>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        # xmltodict usually propagates namespaces.
        # But wait, our `xml_utils` does `etree.tostring(elem)`.
        # lxml preserves the namespace in the string output.
        # If default namespace is used, tags might look like `PMID` in dict if xmltodict handles it,
        # or they might have prefixes if we aren't careful?
        # Actually xmltodict with process_namespaces=False (default) treats xmlns as an attribute.
        # But lxml tostring might inject prefixes like ns0:PMID if not careful.
        # Let's see what happens.
        # In this test case, we want to ensure we get the data out.

        # NOTE: logic in xml_utils uses `etree.tostring(elem, encoding="unicode")`.
        # lxml will likely include the namespace declaration in the output snippet
        # or use a prefix if it was inherited from parent.
        # Since we parse chunks, the chunk (MedlineCitation) will have the namespace.

        # If xmltodict doesn't strip namespaces, the keys might be complex.
        # For this test, we just want to ensure it doesn't crash and we can access data.
        citation = records[0]
        # We need to find the key for MedlineCitation. It might be 'MedlineCitation' or 'ns0:MedlineCitation'.
        # Since we don't know exactly how lxml+xmltodict will serialize the default NS without testing,
        # we inspect the keys.
        keys = list(citation.keys())
        # Filter out _record_type
        xml_keys = [k for k in keys if k != "_record_type"]
        self.assertTrue(len(xml_keys) > 0)
        self.assertTrue("MedlineCitation" in xml_keys[0]) # Should contain the tag name at least

    def test_polymorphism_non_forced_keys(self) -> None:
        """
        Test behavior for keys NOT in FORCE_LIST_KEYS.
        They should be a dict if single, and a list if multiple.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article>
                    <SingleTag>Value1</SingleTag>

                    <MultiTag>ValueA</MultiTag>
                    <MultiTag>ValueB</MultiTag>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        article = records[0]["MedlineCitation"]["Article"]

        # SingleTag should be a string (or dict with #text)
        self.assertEqual(article["SingleTag"], "Value1")

        # MultiTag should be a list
        self.assertIsInstance(article["MultiTag"], list)
        self.assertEqual(len(article["MultiTag"]), 2)
        self.assertEqual(article["MultiTag"][0], "ValueA")
        self.assertEqual(article["MultiTag"][1], "ValueB")

    def test_strip_tags_robustness(self) -> None:
        """
        Ensure strip_tags doesn't crash on edges.
        """
        # <ArticleTitle> with no children but text
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <ArticleTitle>Just Text</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))
        self.assertEqual(records[0]["MedlineCitation"]["Article"]["ArticleTitle"], "Just Text")
