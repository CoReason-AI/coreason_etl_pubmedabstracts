# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import os
import unittest
from unittest.mock import patch, MagicMock
from io import BytesIO
import dlt
from testcontainers.postgres import PostgresContainer
import psycopg2
import subprocess
from urllib.parse import urlparse

# Ensure we use the installed package
from coreason_etl_pubmedabstracts.main import run_pipeline

class TestFullStackIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Spin up Postgres container."""
        cls.postgres = PostgresContainer("postgres:15-alpine")
        cls.postgres.start()

        # Fix URL for dlt (needs postgres:// not postgresql+psycopg2://)
        db_url = cls.postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = db_url

        # Parse connection url to set individual components
        res = urlparse(db_url)

        os.environ["PGHOST"] = res.hostname
        os.environ["PGPORT"] = str(res.port)
        os.environ["PGUSER"] = res.username
        os.environ["PGPASSWORD"] = res.password
        os.environ["PGDATABASE"] = res.path.lstrip('/')
        os.environ["PGSCHEMA"] = "public" # default

        # Set DBT env vars to match profiles.yml
        os.environ["DBT_HOST"] = res.hostname
        os.environ["DBT_PORT"] = str(res.port)
        os.environ["DBT_USER"] = res.username
        os.environ["DBT_PASS"] = res.password
        os.environ["DBT_DBNAME"] = res.path.lstrip('/')
        os.environ["DBT_SCHEMA"] = "public"

    @classmethod
    def tearDownClass(cls):
        cls.postgres.stop()

    def _get_db_connection(self):
        return psycopg2.connect(
            host=os.environ["PGHOST"],
            port=os.environ["PGPORT"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            dbname=os.environ["PGDATABASE"]
        )

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.open_remote_file")
    def test_full_pipeline_execution(self, mock_open, mock_list):
        """
        Runs the full pipeline:
        1. Ingest Baseline (mocked XML).
        2. Ingest Updates (mocked XML, with some overlap).
        3. Run Deduplication (via dbt).
        4. Run dbt transformation (build).
        5. Verify final state.
        """

        # --- Setup Mock Data ---

        # Baseline: 2 records. PMID 100, 200.
        baseline_xml = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID Version="1">100</PMID>
                <Article><ArticleTitle>Baseline 100</ArticleTitle></Article>
            </MedlineCitation>
            <MedlineCitation Status="MEDLINE">
                <PMID Version="1">200</PMID>
                <Article><ArticleTitle>Baseline 200</ArticleTitle></Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """

        # Updates: 3 records.
        # PMID 100 (Overlap - should be deduped out of updates).
        # PMID 300 (New).
        # PMID 200 (DeleteCitation - should result in delete in Staging/Silver).
        update_xml = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID Version="1">100</PMID>
                <Article><ArticleTitle>Update 100 (Should be ignored)</ArticleTitle></Article>
            </MedlineCitation>
            <MedlineCitation Status="MEDLINE">
                <PMID Version="1">300</PMID>
                <Article><ArticleTitle>Update 300</ArticleTitle></Article>
            </MedlineCitation>
            <DeleteCitation>
                <PMID Version="1">200</PMID>
            </DeleteCitation>
        </PubmedArticleSet>
        """

        # Mock side_effect for open_remote_file to return different content based on filename
        def open_side_effect(host, path):
            if "baseline" in path:
                return MagicMock(__enter__=lambda _: BytesIO(baseline_xml))
            elif "update" in path:
                return MagicMock(__enter__=lambda _: BytesIO(update_xml))
            return MagicMock(__enter__=lambda _: BytesIO(b""))

        mock_open.side_effect = open_side_effect

        # Mock list to return our files
        mock_list.side_effect = lambda host, dir, pattern="*.xml.gz": \
            ["/pubmed/baseline/base.xml.gz"] if "baseline" in dir else \
            ["/pubmed/updatefiles/update.xml.gz"]

        # --- Execution ---

        # 1. Run Pipeline (Baseline + Updates + Dedup Sweep)
        run_pipeline(load_target="all")

        # 2. Run dbt models (to materialize views/tables)
        subprocess.run(["dbt", "build", "--project-dir", "dbt_pubmed", "--profiles-dir", "dbt_pubmed"], check=True)

        # --- Verification ---

        conn = self._get_db_connection()
        cur = conn.cursor()

        # Check 1: Bronze Tables Existence
        cur.execute("SELECT count(*) FROM public.bronze_pubmed_baseline")
        count_baseline = cur.fetchone()[0]
        self.assertEqual(count_baseline, 2, "Baseline should have 2 records")

        # Check 2: Deduplication Sweep
        cur.execute("SELECT count(*) FROM public.bronze_pubmed_updates")
        count_updates = cur.fetchone()[0]
        self.assertEqual(count_updates, 2, "Updates should have 2 records (100 removed, 300 and 200-del remain)")

        # Verify content of updates
        cur.execute("""
            SELECT raw_data->'MedlineCitation'->'PMID'->0->>'#text' as pmid
            FROM public.bronze_pubmed_updates
            WHERE raw_data->>'_record_type' = 'citation'
        """)
        update_pmids = [row[0] for row in cur.fetchall()]
        self.assertNotIn("100", update_pmids, "PMID 100 should be deduped from updates")
        self.assertIn("300", update_pmids, "PMID 300 should be in updates")

        # Check 3: Staging Layer (stg_pubmed_citations)
        # Should combine Baseline + Updates
        # Total rows: Baseline(100, 200) + Updates(300, 200-del) = 4 rows in the UNION
        cur.execute("SELECT count(*) FROM public.stg_pubmed_citations")
        count_stg = cur.fetchone()[0]
        self.assertEqual(count_stg, 4)

        cur.close()
        conn.close()

    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.list_remote_files")
    @patch("coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline.open_remote_file")
    def test_complex_scenarios(self, mock_open, mock_list):
        """
        Tests complex edge cases:
        1. Unicode preservation.
        2. FORCE_LIST behavior for single items.
        3. Empty/No-Op files.
        """
        # Complex XML with unicode and single-item list
        complex_xml = b"""
        <PubmedArticleSet>
            <MedlineCitation Status="MEDLINE">
                <PMID Version="1">999</PMID>
                <Article>
                    <ArticleTitle>Caf\xc3\xa9 \xe2\x98\x95 Study</ArticleTitle>
                    <AuthorList>
                        <Author><LastName>Lonely</LastName><ForeName>Author</ForeName></Author>
                    </AuthorList>
                    <PublicationTypeList>
                         <PublicationType>Journal Article</PublicationType>
                    </PublicationTypeList>
                </Article>
            </MedlineCitation>
        </PubmedArticleSet>
        """

        # Empty XML (valid root, no children)
        empty_xml = b"<PubmedArticleSet></PubmedArticleSet>"

        def open_side_effect(host, path):
            if "complex" in path:
                return MagicMock(__enter__=lambda _: BytesIO(complex_xml))
            elif "empty" in path:
                return MagicMock(__enter__=lambda _: BytesIO(empty_xml))
            return MagicMock(__enter__=lambda _: BytesIO(b""))

        mock_open.side_effect = open_side_effect

        mock_list.side_effect = lambda host, dir, pattern="*.xml.gz": \
            ["/pubmed/baseline/complex.xml.gz"] if "baseline" in dir else \
            ["/pubmed/updatefiles/empty.xml.gz"]

        # Run pipeline
        run_pipeline(load_target="all")

        # Run dbt
        subprocess.run(["dbt", "build", "--project-dir", "dbt_pubmed", "--profiles-dir", "dbt_pubmed"], check=True)

        conn = self._get_db_connection()
        cur = conn.cursor()

        # 1. Verify Unicode
        cur.execute("""
            SELECT raw_data->'MedlineCitation'->'Article'->>'ArticleTitle'
            FROM public.bronze_pubmed_baseline
            WHERE raw_data->'MedlineCitation'->'PMID'->0->>'#text' = '999'
        """)
        title = cur.fetchone()[0]
        # "Café ☕ Study"
        self.assertIn("Café", title)
        self.assertIn("☕", title)

        # 2. Verify Force List (AuthorList -> Author should be array)
        cur.execute("""
            SELECT jsonb_typeof(raw_data->'MedlineCitation'->'Article'->'AuthorList'->'Author')
            FROM public.bronze_pubmed_baseline
            WHERE raw_data->'MedlineCitation'->'PMID'->0->>'#text' = '999'
        """)
        auth_type = cur.fetchone()[0]
        self.assertEqual(auth_type, "array", "Author should be an array even if single item")

        # 3. Verify Empty File Handled (No rows from empty file)
        # We expect 1 row total (from complex.xml) in baseline, 0 in updates
        cur.execute("SELECT count(*) FROM public.bronze_pubmed_updates")
        count_updates = cur.fetchone()[0]
        self.assertEqual(count_updates, 0, "Empty file should yield 0 records")

        cur.close()
        conn.close()

if __name__ == "__main__":
    unittest.main()
