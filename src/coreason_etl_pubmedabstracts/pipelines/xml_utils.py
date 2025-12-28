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
# This ensures consistent JSON structure for downstream processing.
FORCE_LIST_KEYS = (
    "Author",
    "ArticleId",
    "Chemical",
    "DataBank",
    "DeleteCitation",  # Though usually wrapper, children PMIDs should be list? No, PMID is the child.
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
    "PMID",  # In DeleteCitation, multiple PMIDs can exist. In MedlineCitation, usually one, but being safe.
    "PublicationType",
    "Reference",
    "SpaceFlightMission",
    "GeneralNote",
    "SupplMeshName",
)


def parse_pubmed_xml(file_stream: IO[bytes]) -> Iterator[Dict[str, Any]]:
    """
    Parse a PubMed XML stream and yield dictionary records.
    Handles both MedlineCitation and DeleteCitation elements.

    Args:
        file_stream: A binary stream of the XML file (uncompressed).

    Yields:
        Dictionary representations of the XML elements.
    """
    # iterparse events: 'end' is sufficient for complete elements.
    # We do not filter by tag in iterparse arguments to handle namespaces robustly.
    context = etree.iterparse(file_stream, events=("end",))

    for _event, elem in context:
        # Strip namespace to check the tag name
        # lxml tags are like "{http://namespace}TagName" or just "TagName"
        tag_name = etree.QName(elem).localname

        if tag_name in ("MedlineCitation", "DeleteCitation"):
            # Convert the lxml element to a string
            xml_str = etree.tostring(elem, encoding="unicode")

            # Parse with xmltodict, forcing specific keys to be lists
            doc = xmltodict.parse(xml_str, force_list=FORCE_LIST_KEYS)

            # Inject _record_type based on the root tag
            if "MedlineCitation" in doc:
                doc["_record_type"] = "citation"
            elif "DeleteCitation" in doc:
                doc["_record_type"] = "delete"

            yield doc

            # Important: Clear the element to save memory
            elem.clear()
            # Also clear the references to previous siblings from the root
            while elem.getprevious() is not None:
                del elem.getparent()[0]
