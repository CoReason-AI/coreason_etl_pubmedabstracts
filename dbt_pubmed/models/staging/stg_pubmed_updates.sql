with source as (
    select * from {{ source('pubmed', 'pubmed_abstract_updates') }}
),

-- Handle Upserts (MedlineCitation)
upserts as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        -- Use macro for extraction
        {{ extract_pubmed_common_fields('source') }},

        'upsert' as operation,
        raw_data
    from source
    where raw_data ->> '_record_type' = 'citation'
),

-- Handle Deletes (DeleteCitation)
deletes as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        -- PMID extraction from exploded array elements
        case
            when jsonb_typeof(pmid_elem) = 'string' then pmid_elem #>> '{}'
            else pmid_elem ->> '#text'
        end as pmid,
        
        -- STANDARD COLUMNS (Set to NULL for deletes)
        null::text as title,
        null::text as abstract_text,
        null::text as pub_year,
        null::text as pub_month,
        null::text as pub_day,
        null::text as medline_date,
        
        -- NEW COLUMNS (Added to match Upserts/Macro) ---------
        null::text as publication_status,
        null::jsonb as publication_type_list,
        null::text as medline_ta,
        null::text as journal_volume,
        null::text as journal_issue,
        null::text as article_pagination_medlinepgn,
        -- ----------------------------------------------------

        null::jsonb as authors,
        null::jsonb as mesh_terms,
        null::jsonb as languages,
        null::text as doi,
        'delete' as operation,
        null::jsonb as raw_data
    from source,
    -- Explode DeleteCitation list
    jsonb_array_elements(coalesce(raw_data -> 'DeleteCitation', '[]'::jsonb)) as dc_obj,
    -- Explode PMID list
    jsonb_array_elements(coalesce(dc_obj -> 'PMID', '[]'::jsonb)) as pmid_elem
    where raw_data ->> '_record_type' = 'delete'
),

combined as (
    select * from upserts
    union all
    select * from deletes
)

select * from combined
