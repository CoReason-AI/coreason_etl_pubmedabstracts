with source as (
    select * from {{ source('bronze', 'bronze_pubmed_raw') }}
),

renamed as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        raw_data,
        -- Extract MedlineCitation if present
        raw_data -> 'MedlineCitation' as medline_citation
    from source
),

parsed as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        -- Extract PMID
        -- Handle both object (with attributes) and simple string cases safely
        case
            when jsonb_typeof(medline_citation -> 'PMID') = 'object' then
                 medline_citation -> 'PMID' ->> '#text'
            else
                 medline_citation ->> 'PMID'
        end as pmid,

        -- Extract DOI
        medline_citation -> 'Article' -> 'ELocationID' as elocation_id,

        -- Title
        -- Handle both object (with attributes) and simple string cases safely
        case
            when jsonb_typeof(medline_citation -> 'Article' -> 'ArticleTitle') = 'object' then
                 medline_citation -> 'Article' -> 'ArticleTitle' ->> '#text'
            else
                 medline_citation -> 'Article' ->> 'ArticleTitle'
        end as title,

        -- Abstract
        medline_citation -> 'Article' -> 'Abstract' -> 'AbstractText' as abstract_raw,

        -- Publication Date Components
        medline_citation -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Year' as pub_year,
        medline_citation -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Month' as pub_month,
        medline_citation -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Day' as pub_day,

        -- Language
        medline_citation -> 'Article' -> 'Language' as language_raw,

        -- Authors
        medline_citation -> 'Article' -> 'AuthorList' -> 'Author' as authors_raw,

        -- MeSH
        medline_citation -> 'MeshHeadingList' -> 'MeshHeading' as mesh_raw

    from renamed
    where medline_citation is not null
),

final as (
    select
        pmid as source_id,
        -- Deterministic UUID generation
        -- Requires uuid-ossp extension
        uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid,
            pmid
        ) as coreason_id,

        -- Extract DOI
        case
            when jsonb_typeof(elocation_id) = 'array' then
                (select item ->> '#text'
                 from jsonb_array_elements(elocation_id) as item
                 where item ->> '@EIdType' = 'doi'
                 limit 1)
            when elocation_id ->> '@EIdType' = 'doi' then elocation_id ->> '#text'
            else null
        end as doi,

        title,

        -- Normalize Abstract
        case
            when jsonb_typeof(abstract_raw) = 'array' then
                (select string_agg(coalesce(item ->> '#text', item #>> '{}'), ' ')
                 from jsonb_array_elements(abstract_raw) as item)
            when jsonb_typeof(abstract_raw) = 'object' then abstract_raw ->> '#text'
            -- Use #>> '{}' to extract scalar text safely if it is a JSON string
            else abstract_raw #>> '{}'
        end as abstract_text,

        -- Publication Date (Robust Handling)
        -- Handles Month as 'Jan', 'Feb' etc. or '01'.
        -- Defaults to Jan 1st if missing.
        case
            when pub_year is null then null
            else
                make_date(
                    pub_year::int,
                    case
                        when pub_month ~ '^\d+$' then pub_month::int
                        when pub_month ilike 'Jan%' then 1
                        when pub_month ilike 'Feb%' then 2
                        when pub_month ilike 'Mar%' then 3
                        when pub_month ilike 'Apr%' then 4
                        when pub_month ilike 'May%' then 5
                        when pub_month ilike 'Jun%' then 6
                        when pub_month ilike 'Jul%' then 7
                        when pub_month ilike 'Aug%' then 8
                        when pub_month ilike 'Sep%' then 9
                        when pub_month ilike 'Oct%' then 10
                        when pub_month ilike 'Nov%' then 11
                        when pub_month ilike 'Dec%' then 12
                        else 1 -- Default to Jan for seasons/unknowns
                    end,
                    coalesce(pub_day::int, 1)
                )
        end as publication_date,

        -- Normalize Language
        {{ normalize_json_list('language_raw') }} as languages,

        -- Normalize Authors and MeSH
        {{ normalize_json_list('authors_raw') }} as authors,
        {{ normalize_json_list('mesh_raw') }} as mesh_terms,

        file_name,
        ingestion_ts,
        content_hash

    from parsed
)

select * from final
