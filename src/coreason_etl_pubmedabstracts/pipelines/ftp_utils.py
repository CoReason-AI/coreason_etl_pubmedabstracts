# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

from typing import IO, Any, List, cast

import fsspec


def list_remote_files(host: str, directory: str, pattern: str = "*.xml.gz") -> List[str]:
    """
    List files in a remote FTP directory matching a pattern.

    Args:
        host: The FTP host (e.g., 'ftp.ncbi.nlm.nih.gov')
        directory: The directory to list (e.g., '/pubmed/baseline/')
        pattern: The glob pattern to match (default: '*.xml.gz')

    Returns:
        A sorted list of file paths.
    """
    # fsspec's FTPFileSystem uses 'ftp' protocol
    # anonymous=True for public FTP
    fs = fsspec.filesystem("ftp", host=host, user="anonymous", password="")

    full_path = f"{directory.rstrip('/')}/{pattern}"

    # glob returns the full paths
    files = fs.glob(full_path)

    # Sort to ensure deterministic order (important for updates)
    return sorted(files)


def open_remote_file(host: str, full_path: str) -> IO[Any]:
    """
    Open a remote file for streaming (reading).
    Handles gzip compression automatically if the file extension ends in .gz.

    Args:
        host: The FTP host.
        full_path: The full path to the file on the FTP server.

    Returns:
        A file-like object compatible with read().
    """
    fs = fsspec.filesystem("ftp", host=host, user="anonymous", password="")

    # compression='infer' is default in open(), but we can be explicit if needed.
    # fsspec usually infers from extension.
    return cast(IO[Any], fs.open(full_path, mode="rb", compression="infer"))
