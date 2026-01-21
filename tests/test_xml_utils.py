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


class TestXmlUtils(unittest.TestCase):
    def test_parse_medline_citation(self) -> None:
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE" Owner="NLM">
                <PMID Version="1">123456</PMID>
                <Article PubModel="Print">
                    <Journal>
                        <ISSN IssnType="Print">0028-4793</ISSN>
                        <Title>New England Journal of Medicine</Title>
                    </Journal>
                    <ArticleTitle>Test Article</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        self.assertIn("MedlineCitation", records[0])
        self.assertEqual(records[0]["_record_type"], "citation")

        # PMID should be a list now because of FORCE_LIST_KEYS
        self.assertIsInstance(records[0]["MedlineCitation"]["PMID"], list)
        self.assertEqual(records[0]["MedlineCitation"]["PMID"][0]["#text"], "123456")

    def test_parse_delete_citation(self) -> None:
        xml_content = b"""
        <PubmedArticleSet>
            <DeleteCitation>
                <PMID Version="1">999999</PMID>
                <PMID Version="1">888888</PMID>
            </DeleteCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        # DeleteCitation is in FORCE_LIST_KEYS, so it is a list
        self.assertIsInstance(records[0]["DeleteCitation"], list)
        self.assertEqual(records[0]["_record_type"], "delete")

        delete_citation = records[0]["DeleteCitation"][0]

        # PMID is in FORCE_LIST_KEYS, so it is a list
        pmids = delete_citation["PMID"]
        self.assertIsInstance(pmids, list)
        self.assertEqual(len(pmids), 2)
        self.assertEqual(pmids[0]["#text"], "999999")

    def test_parse_mixed(self) -> None:
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID>1</PMID>
            </MedlineCitation>
            <DeleteCitation>
                <PMID>2</PMID>
            </DeleteCitation>
            <MedlineCitation Status="MEDLINE">
                <PMID>3</PMID>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 3)
        self.assertIn("MedlineCitation", records[0])
        self.assertEqual(records[0]["_record_type"], "citation")

        self.assertIn("DeleteCitation", records[1])
        self.assertEqual(records[1]["_record_type"], "delete")

        self.assertIn("MedlineCitation", records[2])
        self.assertEqual(records[2]["_record_type"], "citation")

    def test_normalization_force_list(self) -> None:
        """Test that specified keys are always lists even if single."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <PMID>100</PMID>
                <Article>
                    <AuthorList>
                        <Author>
                            <LastName>Doe</LastName>
                            <ForeName>John</ForeName>
                        </Author>
                    </AuthorList>
                    <PublicationTypeList>
                        <PublicationType>Journal Article</PublicationType>
                    </PublicationTypeList>
                </Article>
                <MeshHeadingList>
                    <MeshHeading>
                        <DescriptorName>Science</DescriptorName>
                    </MeshHeading>
                </MeshHeadingList>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        citation = records[0]["MedlineCitation"]

        # Author should be a list
        authors = citation["Article"]["AuthorList"]["Author"]
        self.assertIsInstance(authors, list)
        self.assertEqual(len(authors), 1)
        self.assertEqual(authors[0]["LastName"], "Doe")

        # PublicationType should be a list
        pub_types = citation["Article"]["PublicationTypeList"]["PublicationType"]
        self.assertIsInstance(pub_types, list)
        self.assertEqual(len(pub_types), 1)
        # Without attributes, xmltodict returns the string directly
        self.assertEqual(pub_types[0], "Journal Article")

        # MeshHeading should be a list
        mesh_headings = citation["MeshHeadingList"]["MeshHeading"]
        self.assertIsInstance(mesh_headings, list)
        self.assertEqual(len(mesh_headings), 1)

    def test_mixed_content_flattening(self) -> None:
        """Test that mixed content (e.g., <i>, <b>) is flattened but text is preserved."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <ArticleTitle>
                        Usage of <i>italics</i> and <b>bold</b> in titles.
                    </ArticleTitle>
                    <Abstract>
                        <AbstractText>
                            This is a <sub>subscript</sub> test.
                        </AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        # Whitespace handling in lxml/xmltodict can be tricky, so we strip
        # But generally, it should concatenate text.
        # "Usage of " + "italics" + " and " + "bold" + " in titles."
        self.assertIn("Usage of italics and bold in titles.", title.replace("\n", " ").replace("  ", " "))

        abstract = records[0]["MedlineCitation"]["Article"]["Abstract"]["AbstractText"]
        self.assertIn("This is a subscript test.", abstract.replace("\n", " ").replace("  ", " "))

    def test_namespace_robustness(self) -> None:
        """Test that we can handle namespaces and still flatten mixed content."""
        xml_content = b"""
        <ns:PubmedArticleSet xmlns:ns="http://example.com/ns">
            <ns:MedlineCitation>
                <ns:Article>
                    <ns:ArticleTitle>
                        Namespace <i>mixed</i> content.
                    </ns:ArticleTitle>
                </ns:Article>
            </ns:MedlineCitation>
        </ns:PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        self.assertEqual(len(records), 1)
        # Check if keys have namespaces.
        # We strip namespaces now, so keys should be clean.
        self.assertIn("MedlineCitation", records[0])
        self.assertEqual(records[0]["_record_type"], "citation")

        citation = records[0]["MedlineCitation"]
        title = citation["Article"]["ArticleTitle"]

        # Verify flattening worked (<i> tag stripped)
        self.assertIn("Namespace mixed content.", title.replace("\n", " ").replace("  ", " "))
        # Verify <i> is gone
        self.assertNotIn("<i>", title)

    def test_utf8_encoding(self) -> None:
        """Test that UTF-8 characters are preserved."""
        xml_content = (
            "    <PubmedArticleSet><MedlineCitation><Article><ArticleTitle>Café étude</ArticleTitle></Article>"
            "</MedlineCitation></PubmedArticleSet>".encode("utf-8")
        )

        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        title = records[0]["MedlineCitation"]["Article"]["ArticleTitle"]
        self.assertEqual(title, "Café étude")

    def test_deeply_nested_structure(self) -> None:
        """Test a deep structure with multiple forced lists."""
        xml_content = b"""
        <PubmedArticleSet>
            <MedlineCitation>
                <Article>
                    <AuthorList>
                        <Author ValidYN="Y">
                            <LastName>Smith</LastName>
                            <ForeName>J</ForeName>
                            <Identifier Source="ORCID">0000-0000-0000-0000</Identifier>
                        </Author>
                    </AuthorList>
                </Article>
                <ChemicalList>
                    <Chemical>
                        <NameOfSubstance UI="D000000">TestSubstance</NameOfSubstance>
                    </Chemical>
                </ChemicalList>
            </MedlineCitation>
        </PubmedArticleSet>
        """
        stream = BytesIO(xml_content)
        records = list(parse_pubmed_xml(stream))

        citation = records[0]["MedlineCitation"]
        # ChemicalList -> Chemical (list) -> NameOfSubstance (list)
        # because NameOfSubstance is in FORCE_LIST_KEYS
        substances = citation["ChemicalList"]["Chemical"][0]["NameOfSubstance"]
        self.assertIsInstance(substances, list)
        self.assertEqual(substances[0]["#text"], "TestSubstance")

    def test_parse_empty_stream(self) -> None:
        """Test that an empty stream returns no records and does not crash."""
        stream = BytesIO(b"")
        records = list(parse_pubmed_xml(stream))
        self.assertEqual(records, [])

    def test_parse_malformed_xml(self) -> None:
        """Test that malformed XML raises an XMLSyntaxError."""
        from lxml import etree

        stream = BytesIO(b"<root>unclosed")
        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(stream))

    def test_non_seekable_empty_stream(self) -> None:
        """
        Test that an empty, non-seekable stream is handled gracefully.
        This covers the 'except Exception' block in the seek check
        and the 'except XMLSyntaxError' block in iterparse.
        """

        class NonSeekableStream:
            def __init__(self, data: bytes):
                self._data = BytesIO(data)

            def read(self, size: int = -1) -> bytes:
                return self._data.read(size)

            def seekable(self) -> bool:
                # Raise exception to trigger the try-except block in xml_utils.py
                raise OSError("Checking seekability failed")

            def tell(self) -> int:
                raise OSError("Not seekable")

            def seek(self, offset: int, whence: int = 0) -> int:
                raise OSError("Not seekable")

        stream = NonSeekableStream(b"")
        # Explicitly cast to IO[bytes] because our mock is simple
        from typing import IO, cast

        records = list(parse_pubmed_xml(cast(IO[bytes], stream)))
        self.assertEqual(records, [])

    def test_non_seekable_malformed_xml(self) -> None:
        """Test that a non-seekable malformed stream still raises XMLSyntaxError."""
        from typing import IO, cast

        from lxml import etree

        class NonSeekableStream:
            def __init__(self, data: bytes):
                self._data = BytesIO(data)

            def read(self, size: int = -1) -> bytes:
                return self._data.read(size)

            def seekable(self) -> bool:
                return False

        stream = NonSeekableStream(b"<root>unclosed")
        with self.assertRaises(etree.XMLSyntaxError):
            list(parse_pubmed_xml(cast(IO[bytes], stream)))
