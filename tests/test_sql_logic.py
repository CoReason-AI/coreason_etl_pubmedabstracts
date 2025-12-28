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


class TestSqlLogic(unittest.TestCase):
    """
    These tests verify that the Python parser produces the exact JSON structure
    expected by the SQL logic, especially for edge cases handled by COALESCE/CASE.
    """

    def test_missing_elocation_id(self) -> None:
        """
        Verify that a missing ELocationID tag results in the key being absent
        (or None), which the SQL `coalesce(..., '[]'::jsonb)` must handle.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>2001</PMID>
                <Article>
                    <ArticleTitle>No DOI Paper</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        article = records[0]["MedlineCitation"]["Article"]
        # xmltodict simply omits the key if the tag is missing
        self.assertNotIn("ELocationID", article)

        # In SQL: raw_data -> ... -> 'ELocationID' will be NULL.
        # coalesce(NULL, '[]'::jsonb) -> '[]'.
        # jsonb_array_elements('[]') -> returns 0 rows.
        # Subquery returns NULL.
        # Correct behavior.

    def test_medline_date_parsing(self) -> None:
        """
        Verify the structure of MedlineDate for SQL regex extraction.
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>2002</PMID>
                <Article>
                    <Journal>
                        <JournalIssue>
                            <PubDate>
                                <MedlineDate>1998 Dec-1999 Jan</MedlineDate>
                            </PubDate>
                        </JournalIssue>
                    </Journal>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        pub_date = records[0]["MedlineCitation"]["Article"]["Journal"]["JournalIssue"]["PubDate"]
        self.assertNotIn("Year", pub_date)
        self.assertIn("MedlineDate", pub_date)
        self.assertEqual(pub_date["MedlineDate"], "1998 Dec-1999 Jan")

        # In SQL:
        # raw_data -> ... -> 'Year' IS NULL.
        # raw_data -> ... -> 'MedlineDate' IS "1998 Dec-1999 Jan".
        # substring("1998 Dec-1999 Jan" from '\d{4}') -> "1998".
        # Correct behavior.

    def test_medline_date_mixed(self) -> None:
        """
        Verify behavior when MedlineDate is weird (e.g. "Spring 2000").
        """
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>2003</PMID>
                <Article>
                    <Journal>
                        <JournalIssue>
                            <PubDate>
                                <MedlineDate>Spring 2000</MedlineDate>
                            </PubDate>
                        </JournalIssue>
                    </Journal>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))
        pub_date = records[0]["MedlineCitation"]["Article"]["Journal"]["JournalIssue"]["PubDate"]
        self.assertEqual(pub_date["MedlineDate"], "Spring 2000")
        # SQL substring should find "2000".
