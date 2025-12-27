{{
    config(
        materialized='view'
    )
}}

with baseline as (
    select * from {{ ref('stg_pubmed_baseline') }}
),

updates as (
    select * from {{ ref('stg_pubmed_updates') }}
),

combined as (
    select * from baseline
    union all
    select * from updates
),

ranked as (
    select
        *,
        -- Rank by file_name (alphanumeric sort) to determine the latest state.
        -- Files are named like pubmed24n0001.xml.gz, pubmed24n1001.xml.gz
        -- Higher number = later.
        row_number() over (partition by pmid order by file_name desc, ingestion_ts desc) as rn
    from combined
    where pmid is not null
),

final as (
    select
        pmid as source_id,
        -- Generate deterministic UUID
        uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid,
            pmid
        ) as coreason_id,

        -- Pass through extracted columns
        title,
        abstract_text,
        pub_year,
        authors,
        mesh_terms,
        languages,

        raw_data,
        file_name,
        ingestion_ts
    from ranked
    where rn = 1
    and operation = 'upsert' -- Only keep if the latest operation is an upsert
)

select * from final
