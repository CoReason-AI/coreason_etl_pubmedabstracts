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


class TestExtremeParsingCases(unittest.TestCase):
    """
    Test suite for extreme XML parsing scenarios including recursion, collisions, and CDATA.
    """

    def test_deep_recursion_handling(self) -> None:
        """
        Verify that deeply nested XML does not crash the parser (though lxml has limits).
        We want to ensure we catch the error or handle it if it exceeds limits.
        """
        # Create 1000 nested <Nest> tags
        depth = 1000
        start_tags = "<Nest>" * depth
        end_tags = "</Nest>" * depth
        payload = f"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <Abstract>
                        <AbstractText>{start_tags}Deep{end_tags}</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """.encode("utf-8")

        stream = BytesIO(payload)

        # Depending on lxml configuration, this might work or raise an error.
        # We assume it should pass or raise a specific XML error, but not segfault.
        try:
            records = list(parse_pubmed_xml(stream))
            # If it parses, verify the depth is represented (likely as nested dicts)
            # xmltodict usually handles this.
            self.assertEqual(len(records), 1)
        except etree.XMLSyntaxError:
            # If lxml rejects it, that's also a valid outcome (security)
            pass

    def test_namespace_collision_same_localname(self) -> None:
        """
        Verify that <a:Title> and <b:Title> become a list of 'Title' after namespace stripping.
        """
        payload = b"""
        <PubmedArticleSet xmlns:a="http://a.com" xmlns:b="http://b.com">
            <MedlineCitation>
                <Article>
                    <a:Title>Title A</a:Title>
                    <b:Title>Title B</b:Title>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(payload)
        records = list(parse_pubmed_xml(stream))

        # After stripping namespaces, we have <Title>Title A</Title><Title>Title B</Title>
        # xmltodict should convert this to Title: ['Title A', 'Title B']

        title_field = records[0]["MedlineCitation"]["Article"]["Title"]
        self.assertIsInstance(title_field, list)
        self.assertEqual(len(title_field), 2)
        self.assertIn("Title A", title_field)
        self.assertIn("Title B", title_field)

    def test_cdata_complex_content(self) -> None:
        """
        Verify that CDATA containing XML-like characters is preserved as text.
        """
        cdata_content = "<complicated><xml>structure</xml></complicated>"
        payload = f"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <Abstract>
                        <AbstractText><![CDATA[{cdata_content}]]></AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """.encode("utf-8")

        stream = BytesIO(payload)
        records = list(parse_pubmed_xml(stream))

        abstract_text = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]
        # xmltodict (via lxml) usually transparently handles CDATA as text
        self.assertEqual(abstract_text, cdata_content)

    def test_attribute_element_collision(self) -> None:
        """
        Verify handling when an element has an attribute with the same name as a child element
        (after potential normalization).
        xmltodict uses '@' for attributes, so they shouldn't collide effectively.
        """
        payload = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <!-- Attribute 'Language' -->
                    <Journal Language="English">
                        <!-- Child Element 'Language' -->
                        <Language>French</Language>
                    </Journal>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(payload)
        records = list(parse_pubmed_xml(stream))

        journal = records[0]["MedlineCitation"]["Article"]["Journal"]

        # Attribute becomes @Language
        self.assertEqual(journal["@Language"], "English")

        # Child element becomes Language
        # Note: Language is in FORCE_LIST_KEYS, so it should be a list
        # Check xml_utils.py FORCE_LIST_KEYS
        # "Language" IS in FORCE_LIST_KEYS.

        self.assertIsInstance(journal["Language"], list)
        self.assertEqual(journal["Language"][0], "French")

    def test_mixed_content_with_cdata(self) -> None:
        """
        Verify mixed content flattening when CDATA is involved.
        """
        payload = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <ArticleTitle>
                        Start <![CDATA[<b>Bold</b>]]> End
                    </ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        # _flatten_mixed_content removes child TAGS. CDATA is not a tag, it's text node content.
        # But wait, lxml exposes CDATA as text.
        # So "Start " + "<b>Bold</b>" + " End".
        # The <b> inside CDATA is literal text, not a tag. So strip_tags shouldn't touch it.

        stream = BytesIO(payload)
        records = list(parse_pubmed_xml(stream))

        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        # Whitespace normalization might be needed
        normalized_title = title.replace("\n", "").replace("  ", "")
        self.assertIn("Start <b>Bold</b> End", normalized_title)
