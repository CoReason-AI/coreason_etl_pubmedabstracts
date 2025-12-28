{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['publication_year']}
        ],
        post_hook="-- Partitioning by publication_year is required. Since dbt-postgres doesn't support declarative partitioning in table materialization, this table is indexed on publication_year. For production, convert to a partitioned table manually or via a custom materialization."
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

        -- Ensure safe casting to int
        case
            when pub_year ~ '^\d+$' then (pub_year)::int
            else null
        end as publication_year,

        -- Use default date logic if needed, strictly we just need year for partitioning
        make_date(
            coalesce(
                case when pub_year ~ '^\d+$' then (pub_year)::int else null end,
                1900
            ), 1, 1
        ) as publication_date,

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
        from jsonb_array_elements_text(coalesce(languages, '[]'::jsonb)) as lang
        where lang = '{{ var("FILTER_LANGUAGE", "eng") }}'
    )
    OR '{{ var("FILTER_LANGUAGE", "eng") }}' = ''
    OR '{{ var("FILTER_LANGUAGE", "eng") }}' IS NULL
)
