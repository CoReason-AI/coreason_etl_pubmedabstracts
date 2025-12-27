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
    context = etree.iterparse(file_stream, events=("end",), tag=["MedlineCitation", "DeleteCitation"])

    for _event, elem in context:
        # Convert to dict
        # We use xmltodict.parse on the element's string representation?
        # Or cleaner: convert the element tree to a string then parse?
        # A bit inefficient to serialize back to string.
        # xmltodict works on file-like or string.

        # Optimization: xmltodict doesn't support lxml elements directly efficiently.
        # But we can use elem_to_dict approach or serialize.
        # Given "xmltodict" dependency was requested, let's use it.
        # But for high throughput, we might want to be careful.

        # To use xmltodict with lxml element, we can serialize it.
        xml_str = etree.tostring(elem, encoding="unicode")
        doc = xmltodict.parse(xml_str)

        # doc is {tag: content}. We might want to flatten or wrap it uniformly.
        # For MedlineCitation, the root key is "MedlineCitation".
        # For DeleteCitation, it is "DeleteCitation".

        # We will yield the whole dict with its root key preserved,
        # so dlt can store it as is, or we can wrap it.
        # But usually dlt expects a dict.

        yield doc

        # Important: Clear the element to save memory
        elem.clear()
        # Also clear the references to previous siblings from the root
        while elem.getprevious() is not None:
            del elem.getparent()[0]
