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
import re
from typing import Optional

from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml

# Helper to simulate SQL extracted Year
def extract_year_sql_simulation(pub_date: dict) -> Optional[str]:
    """
    Simulates the SQL logic:
    coalesce(
        raw_data -> ... -> 'Year',
        substring(raw_data -> ... -> 'MedlineDate' from '\d{4}')
    )
    """
    if "Year" in pub_date:
        return pub_date["Year"]

    medline_date = pub_date.get("MedlineDate")
    if medline_date:
        match = re.search(r'\d{4}', medline_date)
        if match:
            return match.group(0)
    return None

class TestAdditionalComplexCases(unittest.TestCase):
    """
    Covers complex scenarios for DeleteCitation lists and Date Parsing edge cases.
    """

    def test_delete_citation_list_structure(self) -> None:
        """
        Verify parsing of DeleteCitation with multiple PMIDs.
        This confirms the FORCE_LIST behavior for DeleteCitation and PMID.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <DeleteCitation>
                <PMID>1001</PMID>
                <PMID>1002</PMID>
                <PMID>1003</PMID>
            </DeleteCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["_record_type"], "delete")

        # DeleteCitation itself is in FORCE_LIST_KEYS, but xmltodict parsing of the root
        # depends on how it is called. Inside parse_pubmed_xml, we convert the *element*
        # to string, so the root of the dict is DeleteCitation.
        # xmltodict.parse(..., force_list=('DeleteCitation', 'PMID'))

        # DeleteCitation is forced to be a list, so we must access the first element
        delete_list = record["DeleteCitation"]
        self.assertIsInstance(delete_list, list)
        delete_node = delete_list[0]

        # Verify PMIDs are a list
        self.assertIn("PMID", delete_node)
        pmids = delete_node["PMID"]
        self.assertIsInstance(pmids, list)
        self.assertEqual(len(pmids), 3)
        self.assertEqual(pmids[0], "1001")
        self.assertEqual(pmids[1], "1002")
        self.assertEqual(pmids[2], "1003")

    def test_medline_date_edge_cases(self) -> None:
        """
        Test various MedlineDate formats to verify SQL extraction logic assumptions.
        """
        cases = [
            ("2000 Spring", "2000"),
            ("Winter 2001", "2001"),
            ("1999 Dec-2000 Jan", "1999"), # Regex finds first 4 digits
            ("2005 May 23-25", "2005"),
            ("2008 Oct-Nov", "2008"),
            ("Copyright 2010", "2010"), # Unlikely but tests regex
        ]

        for date_str, expected_year in cases:
            with self.subTest(date_str=date_str):
                xml_content = f"""
                <PubmedArticleSet>
                    <MedlineCitation>
                        <PMID>9999</PMID>
                        <Article>
                            <Journal>
                                <JournalIssue>
                                    <PubDate>
                                        <MedlineDate>{date_str}</MedlineDate>
                                    </PubDate>
                                </JournalIssue>
                            </Journal>
                        </Article>
                    </MedlineCitation>
                </PubmedArticleSet>
                """.encode('utf-8')

                stream = BytesIO(xml_content)
                records = list(parse_pubmed_xml(stream))
                pub_date = records[0]["MedlineCitation"]["Article"]["Journal"]["JournalIssue"]["PubDate"]

                extracted_year = extract_year_sql_simulation(pub_date)
                self.assertEqual(extracted_year, expected_year)

    def test_publication_year_dirty_data(self) -> None:
        """
        Verify that our SQL regex safe cast logic would handle dirty data.
        This test simulates the SQL logic: case when pub_year ~ '^\d+$' ...
        """

        def safe_cast_year_simulation(year_val: str) -> Optional[int]:
            # Regex: ^\d+$
            if re.match(r'^\d+$', year_val):
                return int(year_val)
            return None

        self.assertEqual(safe_cast_year_simulation("2023"), 2023)
        self.assertIsNone(safe_cast_year_simulation("2023a"))
        self.assertIsNone(safe_cast_year_simulation("2023-01")) # Strict digit check
        self.assertIsNone(safe_cast_year_simulation("Unknown"))

    def test_mixed_content_flattening_complex(self) -> None:
        """
        Verify aggressive flattening of nested mixed content (e.g. sup, sub, i, b).
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>8888</PMID>
                <Article>
                    <ArticleTitle>
                        Effects of <i>H. pylori</i> on <sup>13</sup>C-urea breath test
                    </ArticleTitle>
                    <Abstract>
                        <AbstractText>
                            We used <b>bold</b> and <sub>subscript</sub> logic.
                        </AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        # Verify title
        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        # Expected: tags stripped, text preserved, whitespace might be messy depending on parser
        # lxml.strip_tags preserves text.
        # "Effects of H. pylori on 13C-urea breath test"
        # We need to account for whitespace that lxml might leave (newlines/indents).
        self.assertIn("Effects of", title)
        self.assertIn("H. pylori", title)
        self.assertIn("13C-urea", title)

        # Verify abstract
        abstract = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]
        self.assertIn("We used bold and subscript logic.", " ".join(abstract.split()))

    def test_language_array_handling(self) -> None:
        """
        Verify that Language is forced to a list, enabling the SQL UNNEST logic.
        """
        # Case 1: Single Language
        xml_1 = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>1</PMID>
                <Article><Language>eng</Language></Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_1)
        rec = list(parse_pubmed_xml(stream))[0]
        langs = rec["MedlineCitation"]["Article"]["Language"]
        self.assertIsInstance(langs, list)
        self.assertEqual(langs[0], "eng")

        # Case 2: Multiple Languages
        xml_2 = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>2</PMID>
                <Article>
                    <Language>eng</Language>
                    <Language>fra</Language>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_2)
        rec = list(parse_pubmed_xml(stream))[0]
        langs = rec["MedlineCitation"]["Article"]["Language"]
        self.assertIsInstance(langs, list)
        self.assertEqual(len(langs), 2)
        self.assertIn("fra", langs)
