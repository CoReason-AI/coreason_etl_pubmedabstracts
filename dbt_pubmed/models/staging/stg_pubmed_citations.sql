{{
    config(
        materialized='view'
    )
}}

with baseline as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        pmid,
        title,
        abstract_text,
        pub_year,
        pub_month,
        pub_day,
        medline_date,
        -- New Columns
        publication_status,
        publication_type_list,
        medline_ta,
        journal_volume,
        journal_issue,
        article_pagination_medlinepgn,
        --
        authors,
        mesh_terms,
        languages,
        doi,
        operation,
        raw_data
    from {{ ref('stg_pubmed_baseline') }}
),

updates as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        pmid,
        title,
        abstract_text,
        pub_year,
        pub_month,
        pub_day,
        medline_date,
        -- New Columns
        publication_status,
        publication_type_list,
        medline_ta,
        journal_volume,
        journal_issue,
        article_pagination_medlinepgn,
        --
        authors,
        mesh_terms,
        languages,
        doi,
        operation,
        raw_data
    from {{ ref('stg_pubmed_updates') }}
),

unioned as (
    select * from baseline
    union all
    select * from updates
)

select * from unioned
