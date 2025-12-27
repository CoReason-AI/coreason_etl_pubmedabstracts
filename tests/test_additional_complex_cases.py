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
    def test_deeply_nested_variance(self) -> None:
        """
        Verify list/dict variance for deeply nested elements.
        Example: Article -> PublicationTypeList -> PublicationType
        Record 1: One PublicationType (dict)
        Record 2: Two PublicationTypes (list)
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID>1</PMID>
                <Article>
                    <PublicationTypeList>
                        <PublicationType UI="D016428">Journal Article</PublicationType>
                    </PublicationTypeList>
                </Article>
            </MedlineCitation>
            <MedlineCitation Status="MEDLINE">
                <PMID>2</PMID>
                <Article>
                    <PublicationTypeList>
                        <PublicationType UI="D016428">Journal Article</PublicationType>
                        <PublicationType UI="D016454">Review</PublicationType>
                    </PublicationTypeList>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 2)

        # Record 1
        pt1 = records[0]["MedlineCitation"]["Article"]["PublicationTypeList"]["PublicationType"]
        self.assertIsInstance(pt1, dict)
        self.assertEqual(pt1["#text"], "Journal Article")
        self.assertEqual(pt1["@UI"], "D016428")

        # Record 2
        pt2 = records[1]["MedlineCitation"]["Article"]["PublicationTypeList"]["PublicationType"]
        self.assertIsInstance(pt2, list)
        self.assertEqual(len(pt2), 2)
        self.assertEqual(pt2[0]["#text"], "Journal Article")
        self.assertEqual(pt2[1]["#text"], "Review")

    def test_html_entities(self) -> None:
        """
        Verify correct decoding of entities like &amp;, &lt;, &gt;, &quot;, &apos;.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>100</PMID>
                <Article>
                    <ArticleTitle>Tom &amp; Jerry: &quot;The Movie&quot; &lt;Sequeal&gt;</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        self.assertEqual(title, 'Tom & Jerry: "The Movie" <Sequeal>')

    def test_comments_and_processing_instructions(self) -> None:
        """
        Verify that comments and processing instructions do not interfere with parsing.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <!-- This is a comment -->
            <?xml-stylesheet type="text/xsl" href="style.xsl"?>
            <MedlineCitation Status="MEDLINE">
                <PMID>1</PMID>
                <!-- Nested comment -->
                <Article>
                    <ArticleTitle>Title</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["MedlineCitation"]["PMID"], "1")
        # Ensure comments are not in the dict (xmltodict ignores them by default unless configured otherwise)
        self.assertNotIn("#comment", records[0]["MedlineCitation"])

    def test_mixed_content_with_attributes(self) -> None:
        """
        Verify elements with both text content and attributes are parsed as dict with #text and @attr.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article>
                    <Abstract>
                        <AbstractText Label="BACKGROUND" NlmCategory="BACKGROUND">Background text.</AbstractText>
                        <AbstractText Label="RESULTS" NlmCategory="RESULTS">Results text.</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        abstract_texts = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]
        self.assertIsInstance(abstract_texts, list)

        self.assertEqual(abstract_texts[0]["@Label"], "BACKGROUND")
        self.assertEqual(abstract_texts[0]["#text"], "Background text.")
        self.assertEqual(abstract_texts[1]["@Label"], "RESULTS")
        self.assertEqual(abstract_texts[1]["#text"], "Results text.")

    def test_large_stream_simulation(self) -> None:
        """
        Simulate processing a stream with multiple records to ensure the iterator works correctly over time.
        """
        # Create a repeated XML structure
        single_record = b"<MedlineCitation><PMID>1</PMID></MedlineCitation>"
        xml_content = b"<PubmedArticleSet>" + (single_record * 100) + b"</PubmedArticleSet>"

        stream = BytesIO(xml_content)
        records_iter = parse_pubmed_xml(stream)

        count = 0
        for record in records_iter:
            count += 1
            self.assertEqual(record["MedlineCitation"]["PMID"], "1")

        self.assertEqual(count, 100)

    def test_attributes_collision(self) -> None:
        """
        Verify handling of attributes that share names with child tags.
        (though unlikely in valid XML without namespaces).
        xmltodict prefixes attributes with '@', so collision is avoided.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="OK">
                <Status>Nested Status</Status>
                <PMID>1</PMID>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        citation = records[0]["MedlineCitation"]
        self.assertEqual(citation["@Status"], "OK")
        self.assertEqual(citation["Status"], "Nested Status")

    def test_empty_attributes(self) -> None:
        """
        Verify handling of empty attributes.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="">
                <PMID>1</PMID>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        # xmltodict usually preserves empty string
        self.assertEqual(records[0]["MedlineCitation"]["@Status"], "")
