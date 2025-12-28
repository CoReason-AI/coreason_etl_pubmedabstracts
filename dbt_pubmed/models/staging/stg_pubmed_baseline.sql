with source as (
    select * from {{ source('pubmed', 'bronze_pubmed_baseline') }}
),

parsed as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        raw_data,
        -- Extract common fields safely handling both string and object variants
        -- PMID
        case
            when jsonb_typeof(raw_data -> 'MedlineCitation' -> 'PMID' -> 0) = 'string' then
                 raw_data -> 'MedlineCitation' -> 'PMID' ->> 0
            else
                 raw_data -> 'MedlineCitation' -> 'PMID' -> 0 ->> '#text'
        end as pmid,

        -- Title
        -- ArticleTitle might be a string or object (if it has attributes)
        case
            when jsonb_typeof(raw_data -> 'MedlineCitation' -> 'Article' -> 'ArticleTitle') = 'string' then
                 raw_data -> 'MedlineCitation' -> 'Article' ->> 'ArticleTitle'
            else
                 raw_data -> 'MedlineCitation' -> 'Article' -> 'ArticleTitle' ->> '#text'
        end as title,

        -- Abstract
        -- AbstractText can be complex.
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
        -- Fallback to regex extraction from MedlineDate if Year is missing
        coalesce(
            raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Year',
            substring(raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'MedlineDate' from '\d{4}')
        ) as pub_year,

        -- Authors (Already normalized to list in Python)
        -- Path: Article -> AuthorList -> Author
        raw_data -> 'MedlineCitation' -> 'Article' -> 'AuthorList' -> 'Author' as authors,

        -- MeSH (Already normalized to list in Python)
        raw_data -> 'MedlineCitation' -> 'MeshHeadingList' -> 'MeshHeading' as mesh_terms,

        -- Language
        raw_data -> 'MedlineCitation' -> 'Article' -> 'Language' as languages,

        -- DOI Extraction
        -- ELocationID is forced as a list. We search for the element with EIdType="doi".
        -- Coalesce to empty array to handle cases where ELocationID is missing.
        (
            select item ->> '#text'
            from jsonb_array_elements(coalesce(raw_data -> 'MedlineCitation' -> 'Article' -> 'ELocationID', '[]'::jsonb)) as item
            where item ->> '@EIdType' = 'doi'
            limit 1
        ) as doi,

        'upsert' as operation

    from source
    where raw_data ->> '_record_type' = 'citation'
)

select * from parsed
