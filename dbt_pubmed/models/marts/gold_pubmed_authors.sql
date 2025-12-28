{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ ref('int_pubmed_deduped') }}
),

flattened_authors as (
    select
        source_id,
        coreason_id,

        -- Explode authors array
        jsonb_array_elements(authors) as author_json

    from source
    where authors is not null
    and jsonb_typeof(authors) = 'array'
),

final as (
    select
        coreason_id,
        source_id,

        -- Generate deterministic Author ID based on name components
        -- Using DNS namespace: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
        uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid,
            coalesce(author_json ->> 'LastName', '') || '|' ||
            coalesce(author_json ->> 'ForeName', '') || '|' ||
            coalesce(author_json ->> 'Initials', '')
        ) as author_id,

        -- Extract fields from author object
        -- Handling potential variance (e.g. LastName, ForeName, Initials)
        author_json ->> 'LastName' as last_name,
        author_json ->> 'ForeName' as fore_name,
        author_json ->> 'Initials' as initials,

        -- Affiliation might be nested or a list
        -- Taking the first one if it's a list or text if it's a string
        case
            when jsonb_typeof(author_json -> 'AffiliationInfo') = 'array' then
                author_json -> 'AffiliationInfo' -> 0 ->> 'Affiliation'
            when jsonb_typeof(author_json -> 'AffiliationInfo') = 'object' then
                 author_json -> 'AffiliationInfo' ->> 'Affiliation'
            else null
        end as affiliation

    from flattened_authors
)

select * from final
