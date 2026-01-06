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


class TestFrankensteinXml(unittest.TestCase):
    def test_frankenstein_record(self) -> None:
        """
        A single test case combining multiple edge cases:
        1. Namespaces (nested and default)
        2. Mixed Content (bold/italic in text)
        3. Force List Keys (Author, Chemical)
        4. Attributes on Elements
        5. CDATA
        6. UTF-8 Characters
        7. Self-closing tags
        """
        # We use a standard string with f-string and then encode to utf-8 bytes
        xml_string = """
        <PubmedArticleSet xmlns:ns="http://example.org/ns">
            <MedlineCitation Status="MEDLINE" Owner="NLM">
                <PMID>99999</PMID>
                <ns:DateCreated>
                    <ns:Year>2024</ns:Year>
                    <ns:Month>01</ns:Month>
                    <ns:Day>01</ns:Day>
                </ns:DateCreated>
                <Article PubModel="Print">
                    <Journal>
                        <ISSN IssnType="Print">1234-5678</ISSN>
                        <JournalIssue CitedMedium="Print">
                            <Volume>100</Volume>
                            <Issue>1</Issue>
                            <PubDate>
                                <Year>2024</Year>
                            </PubDate>
                        </JournalIssue>
                        <Title>Journal of Chaos 🧪</Title>
                        <ISOAbbreviation>J. Chaos</ISOAbbreviation>
                    </Journal>
                    <ArticleTitle>A study of <b>bold</b> and <ns:i>italic</ns:i> mixed content with namespaces.</ArticleTitle>
                    <Abstract>
                        <AbstractText Label="BACKGROUND" NlmCategory="BACKGROUND">
                            Here is some <![CDATA[CDATA content]]> and a complex chemical.
                        </AbstractText>
                    </Abstract>
                    <AuthorList CompleteYN="Y">
                        <Author ValidYN="Y">
                            <LastName>Doe</LastName>
                            <ForeName>John</ForeName>
                            <Initials>J</Initials>
                        </Author>
                    </AuthorList>
                    <Language>eng</Language>
                    <ChemicalList>
                        <Chemical>
                            <RegistryNumber>0</RegistryNumber>
                            <NameOfSubstance UI="D012345">Complex Substance</NameOfSubstance>
                        </Chemical>
                    </ChemicalList>
                    <CitationSubset>IM</CitationSubset>
                    <MeshHeadingList>
                        <MeshHeading>
                            <DescriptorName UI="D006801" MajorTopicYN="N">Humans</DescriptorName>
                        </MeshHeading>
                    </MeshHeadingList>
                    <EmptyTag/>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_string.encode("utf-8"))
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        record = records[0]
        citation = record["MedlineCitation"]

        # 1. Namespaces should be stripped
        self.assertIn("DateCreated", citation)
        self.assertNotIn("ns:DateCreated", citation)
        self.assertEqual(citation["DateCreated"]["Year"], "2024")

        # 2. Mixed Content should be flattened
        # "A study of bold and italic mixed content with namespaces."
        title = citation["Article"]["ArticleTitle"]
        self.assertIn("A study of bold and italic mixed content", title)

        # 3. Force List Keys
        authors = citation["Article"]["AuthorList"]["Author"]
        self.assertIsInstance(authors, list)
        self.assertEqual(len(authors), 1)
        self.assertEqual(authors[0]["LastName"], "Doe")

        # 4. Attributes
        self.assertEqual(citation["@Status"], "MEDLINE")
        abstract = citation["Article"]["Abstract"]["AbstractText"]
        self.assertEqual(abstract["@Label"], "BACKGROUND")
        # CDATA text content
        self.assertIn("CDATA content", abstract["#text"])

        # 6. UTF-8
        journal_title = citation["Article"]["Journal"]["Title"]
        self.assertEqual(journal_title, "Journal of Chaos 🧪")

        # 7. Self Closing Tag
        self.assertIsNone(citation["Article"]["EmptyTag"])
