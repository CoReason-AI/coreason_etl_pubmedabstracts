# Copyright (c) 2025 CoReason, Inc.
# ... [License Header] ...

from typing import IO, Any, Dict, Iterator

import xmltodict
from lxml import etree

# Keys that should always be parsed as a list, even if only one element exists.
FORCE_LIST_KEYS = (
    "Author",
    "ArticleId",
    "Chemical",
    "DataBank",
    "DeleteCitation",
    "ELocationID",
    "GeneSymbol",
    "Grant",
    "Investigator",
    "Keyword",
    "Language",
    "MeshHeading",
    "NameOfSubstance",
    "Object",
    "OtherAbstract",
    "OtherID",
    "PersonalNameSubject",
    "PMID",
    "PublicationType",
    "Reference",
    "SpaceFlightMission",
    "GeneralNote",
    "SupplMeshName",
)


def _strip_namespaces(elem: etree._Element) -> None:
    """
    Remove namespaces from the element and its children in-place.
    Also removes xmlns attributes.
    """
    for node in elem.iter():
        if isinstance(node.tag, str) and "}" in node.tag:
            node.tag = etree.QName(node).localname
    etree.cleanup_namespaces(elem)


def _flatten_mixed_content(elem: etree._Element, tags: tuple[str, ...]) -> None:
    """
    Flatten mixed content (remove child tags but keep text) for specified tags.
    """
    for tag in tags:
        for node in elem.xpath(f".//*[local-name()='{tag}']"):
            etree.strip_tags(node, "*")


def parse_pubmed_xml(file_stream: IO[bytes]) -> Iterator[Dict[str, Any]]:
    """
    Parse a PubMed XML stream and yield dictionary records.
    Handles both PubmedArticle (containing MedlineCitation+PubmedData) and DeleteCitation.
    """
    try:
        if file_stream.seekable():
            pos = file_stream.tell()
            if not file_stream.read(1):
                return
            file_stream.seek(pos)
    except Exception:
        pass

    try:
        # Update: Listen for PubmedArticle instead of MedlineCitation to capture PubmedData
        context = etree.iterparse(file_stream, events=("end",))

        for _event, elem in context:
            tag_name = etree.QName(elem).localname

            if tag_name in ("PubmedArticle", "DeleteCitation"):
                # 1. Flatten mixed content (still applies to children like AbstractText)
                _flatten_mixed_content(
                    elem,
                    ("ArticleTitle", "AbstractText", "VernacularTitle", "Affiliation"),
                )

                # 2. Strip Namespaces
                _strip_namespaces(elem)

                # 3. Convert to String
                xml_str = etree.tostring(elem, encoding="unicode")

                # 4. Parse to Dict
                doc = xmltodict.parse(xml_str, force_list=FORCE_LIST_KEYS)

                # 5. Inject Record Type
                if tag_name == "PubmedArticle":
                    doc["_record_type"] = "citation"
                elif tag_name == "DeleteCitation":
                    doc["_record_type"] = "delete"

                yield doc

                # 6. Memory Management
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

    except etree.XMLSyntaxError as e:
        if "no element found" in str(e):
            return
        raise
