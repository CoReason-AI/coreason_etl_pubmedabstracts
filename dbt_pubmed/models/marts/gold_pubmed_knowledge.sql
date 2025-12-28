{{
    config(
        materialized='table',
        partition_by={
            "field": "publication_year",
            "data_type": "int4"
        }
    )
}}

with source as (
    select * from {{ ref('int_pubmed_deduped') }}
),

final as (
    select
        source_id,
        coreason_id,
        title,
        doi,

        abstract_text,

        (pub_year)::int as publication_year,

        -- Use default date logic if needed, strictly we just need year for partitioning
        make_date(coalesce((pub_year)::int, 1900), 1, 1) as publication_date,

        authors,
        mesh_terms,
        languages,

        file_name,
        ingestion_ts

    from source
)

select *
from final
where abstract_text is not null
and (
    -- Language Filter
    -- Check if 'eng' is in the languages list or if it matches
    -- Assuming languages is a list of strings due to FORCE_LIST_KEYS
    exists (
        select 1
        from jsonb_array_elements_text(languages) as lang
        where lang = 'eng'
    )
    OR '{{ var("FILTER_LANGUAGE", "eng") }}' IS NULL
)
