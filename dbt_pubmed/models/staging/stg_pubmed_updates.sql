with source as (
    select * from {{ source('pubmed', 'bronze_pubmed_updates') }}
),

-- Handle Upserts (MedlineCitation)
upserts as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        -- PMID extraction
        case
            when jsonb_typeof(raw_data -> 'MedlineCitation' -> 'PMID' -> 0) = 'string' then
                 raw_data -> 'MedlineCitation' -> 'PMID' ->> 0
            else
                 raw_data -> 'MedlineCitation' -> 'PMID' -> 0 ->> '#text'
        end as pmid,

        -- Title
        case
            when jsonb_typeof(raw_data -> 'MedlineCitation' -> 'Article' -> 'ArticleTitle') = 'string' then
                 raw_data -> 'MedlineCitation' -> 'Article' ->> 'ArticleTitle'
            else
                 raw_data -> 'MedlineCitation' -> 'Article' -> 'ArticleTitle' ->> '#text'
        end as title,

        -- Abstract
        case
            when jsonb_typeof(raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') = 'string' then
                 raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' ->> 'AbstractText'
            when jsonb_typeof(raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') = 'array' then
                (select string_agg(coalesce(item ->> '#text', item #>> '{}'), ' ')
                 from jsonb_array_elements(raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') as item)
            else
                 raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText' ->> '#text'
        end as abstract_text,

        -- Publication Year
        coalesce(
            raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Year',
            substring(raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'MedlineDate' from '\d{4}')
        ) as pub_year,

        -- Authors
        raw_data -> 'MedlineCitation' -> 'Article' -> 'AuthorList' -> 'Author' as authors,

        -- MeSH
        raw_data -> 'MedlineCitation' -> 'MeshHeadingList' -> 'MeshHeading' as mesh_terms,

        -- Language
        raw_data -> 'MedlineCitation' -> 'Article' -> 'Language' as languages,

        -- DOI Extraction
        (
            select item ->> '#text'
            from jsonb_array_elements(coalesce(raw_data -> 'MedlineCitation' -> 'Article' -> 'ELocationID', '[]'::jsonb)) as item
            where item ->> '@EIdType' = 'doi'
            limit 1
        ) as doi,

        'upsert' as operation,
        raw_data
    from source
    where raw_data ? 'MedlineCitation'
),

-- Handle Deletes (DeleteCitation)
-- DeleteCitation is a list (FORCE_LIST_KEYS), and contains PMIDs which are lists.
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
        null::text as title,
        null::text as abstract_text,
        null::text as pub_year,
        null::jsonb as authors,
        null::jsonb as mesh_terms,
        null::jsonb as languages,
        null::text as doi,
        'delete' as operation,
        null::jsonb as raw_data
    from source,
    -- Explode DeleteCitation list (usually one item but strictly it's a list)
    jsonb_array_elements(raw_data -> 'DeleteCitation') as dc_obj,
    -- Explode PMID list inside the DeleteCitation object
    jsonb_array_elements(dc_obj -> 'PMID') as pmid_elem
    where raw_data ? 'DeleteCitation'
),

combined as (
    select * from upserts
    union all
    select * from deletes
)

select * from combined
