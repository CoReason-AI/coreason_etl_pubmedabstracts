{{
    config(
        materialized='partitioned_table',
        partition_by='publication_year',
        indexes=[
            {'columns': ['publication_year']}
        ]
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

        -- Use calculated date from Silver layer
        publication_date,

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
