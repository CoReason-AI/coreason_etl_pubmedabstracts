# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

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
    # Iterate over all elements in the subtree
    for node in elem.iter():
        # Check if the tag has a namespace (indicated by '}')
        if isinstance(node.tag, str) and "}" in node.tag:
            # Replace tag with its local name
            node.tag = etree.QName(node).localname

    # Remove unused namespace declarations
    etree.cleanup_namespaces(elem)


def _flatten_mixed_content(elem: etree._Element, tags: tuple[str, ...]) -> None:
    """
    Flatten mixed content (remove child tags but keep text) for specified tags.
    """
    for tag in tags:
        # Use local-name() to find tags regardless of namespace (though we strip them later)
        for node in elem.xpath(f".//*[local-name()='{tag}']"):
            etree.strip_tags(node, "*")


def parse_pubmed_xml(file_stream: IO[bytes]) -> Iterator[Dict[str, Any]]:
    """
    Parse a PubMed XML stream and yield dictionary records.
    Handles both MedlineCitation and DeleteCitation elements.

    Args:
        file_stream: A binary stream of the XML file (uncompressed).

    Yields:
        Dictionary representations of the XML elements.
    """
    # Check for empty stream to prevent XMLSyntaxError
    try:
        if file_stream.seekable():
            pos = file_stream.tell()
            if not file_stream.read(1):
                return
            file_stream.seek(pos)
    except Exception:
        # Stream might not be seekable, continue
        pass

    try:
        # iterparse events: 'end' is sufficient for complete elements.
        context = etree.iterparse(file_stream, events=("end",))

        for _event, elem in context:
            # Check tag name robustly (ignoring namespace prefix)
            tag_name = etree.QName(elem).localname

            if tag_name in ("MedlineCitation", "DeleteCitation"):
                # 1. Flatten mixed content for text-heavy fields
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
                if tag_name == "MedlineCitation":
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
