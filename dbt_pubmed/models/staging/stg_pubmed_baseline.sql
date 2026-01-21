with source as (
    select * from {{ source('pubmed', 'pubmed_abstract_baseline') }}
),

parsed as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        raw_data,
        -- Use macro for extraction
        {{ extract_pubmed_common_fields('source') }},

        'upsert' as operation

    from source
    where raw_data ->> '_record_type' = 'citation'
)

select * from parsed
