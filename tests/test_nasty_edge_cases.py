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


class TestNastyEdgeCases(unittest.TestCase):
    """
    Test suite for security, stability, and extreme edge cases.
    """

    def test_billion_laughs_attack(self) -> None:
        """
        Verify that XML Entity Expansion (Billion Laughs) is blocked or handled
        safely by lxml default configuration (which usually limits entity expansion).
        """
        # A classic Billion Laughs attack payload
        payload = b"""<?xml version="1.0"?>
        <!DOCTYPE lolz [
         <!ENTITY lol "lol">
         <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
         <!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;">
         <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">
         <!ENTITY lol4 "&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;">
         <!ENTITY lol5 "&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;">
         <!ENTITY lol6 "&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;">
         <!ENTITY lol7 "&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;">
         <!ENTITY lol8 "&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;">
         <!ENTITY lol9 "&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;">
        ]>
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <Abstract>
                        <AbstractText>&lol9;</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(payload)

        # lxml.etree.iterparse should raise an XMLSyntaxError due to entity expansion limits
        # or just fail to parse safely.
        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(stream))

    def test_unicode_surrogate_pairs(self) -> None:
        """
        Test that 4-byte Unicode characters (e.g., Emoji 🧪) are preserved exactly.
        """
        # "🧪" is U+1F9EA (Test Tube)
        emoji_text = "Science is cool 🧪 and so are DNA strands 🧬."
        payload = f"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <ArticleTitle>{emoji_text}</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """.encode("utf-8")

        stream = BytesIO(payload)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        self.assertEqual(title, emoji_text)

    def test_sql_injection_simulation(self) -> None:
        """
        Ensure that strings looking like SQL Injection are treated as plain text
        and not corrupted during parsing.
        """
        nasty_string = "'; DROP TABLE users; --"
        payload = f"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <Abstract>
                        <AbstractText>{nasty_string}</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """.encode("utf-8")

        stream = BytesIO(payload)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        abstract = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]
        self.assertEqual(abstract, nasty_string)

    def test_dynamic_list_upgrade(self) -> None:
        """
        Verify behavior when a key NOT in FORCE_LIST_KEYS appears multiple times.
        xmltodict should automatically upgrade it to a list.
        """
        payload = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <CustomField>Value 1</CustomField>
                    <CustomField>Value 2</CustomField>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(payload)
        records = list(parse_pubmed_xml(stream))

        # "CustomField" is not in the hardcoded FORCE_LIST_KEYS
        custom_field = records[0]["MedlineCitation"]["Article"]["CustomField"]

        # Expectation: It's a list because multiple siblings exist
        self.assertIsInstance(custom_field, list)
        self.assertEqual(len(custom_field), 2)
        self.assertEqual(custom_field[0], "Value 1")
        self.assertEqual(custom_field[1], "Value 2")

    def test_large_payload_limit_enforced(self) -> None:
        """
        Verify that we STRICTLY REJECT payloads with text nodes exceeding default limits (10MB).
        This ensures protection against DoS memory exhaustion.
        """
        large_text = "A" * (11 * 1024 * 1024)  # 11 MB string (Limit is 10MB)
        payload = f"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <Abstract>
                        <AbstractText>{large_text}</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """.encode("utf-8")

        stream = BytesIO(payload)

        # We expect lxml to raise XMLSyntaxError: Text node too long
        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(stream))

    def test_large_valid_payload(self) -> None:
        """
        Verify that a reasonably large payload (e.g., 2MB) is accepted.
        """
        large_text = "A" * (2 * 1024 * 1024)  # 2 MB string
        payload = f"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <Abstract>
                        <AbstractText>{large_text}</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """.encode("utf-8")

        stream = BytesIO(payload)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        abstract = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]
        self.assertEqual(len(abstract), len(large_text))
