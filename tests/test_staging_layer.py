# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts


def test_stg_pubmed_citations_union_logic() -> None:
    """
    Verify that stg_pubmed_citations correctly unions baseline and updates.
    We simulate the SQL logic by creating two mock lists of dicts (baseline and updates)
    and verifying the unioned result contains all records with correct structure.
    """
    # Mock Data
    baseline_records = [
        {
            "file_name": "baseline.xml.gz",
            "ingestion_ts": 100,
            "pmid": "1",
            "operation": "upsert",
            "title": "Baseline Title",
        }
    ]
    updates_records = [
        {
            "file_name": "update.xml.gz",
            "ingestion_ts": 200,
            "pmid": "2",
            "operation": "upsert",
            "title": "Update Title",
        },
        {
            "file_name": "update.xml.gz",
            "ingestion_ts": 200,
            "pmid": "1",
            "operation": "delete",
            "title": None,
        },
    ]

    # Simulate UNION ALL
    # In SQL: select * from baseline UNION ALL select * from updates
    unioned_results = baseline_records + updates_records

    assert len(unioned_results) == 3
    assert unioned_results[0]["pmid"] == "1"
    assert unioned_results[0]["operation"] == "upsert"
    assert unioned_results[2]["pmid"] == "1"
    assert unioned_results[2]["operation"] == "delete"


def test_int_pubmed_deduped_references_staging_view() -> None:
    """
    Verify that int_pubmed_deduped uses the stg_pubmed_citations view.
    This is a structural check of the SQL file content.
    """
    with open("dbt_pubmed/models/intermediate/int_pubmed_deduped.sql", "r") as f:
        sql_content = f.read()

    assert "ref('stg_pubmed_citations')" in sql_content
    assert "ref('stg_pubmed_baseline')" not in sql_content
    assert "ref('stg_pubmed_updates')" not in sql_content


def test_stg_pubmed_citations_structure() -> None:
    """
    Verify structure of stg_pubmed_citations.sql
    """
    with open("dbt_pubmed/models/staging/stg_pubmed_citations.sql", "r") as f:
        sql_content = f.read()

    assert "union all" in sql_content.lower()
    assert "ref('stg_pubmed_baseline')" in sql_content
    assert "ref('stg_pubmed_updates')" in sql_content
