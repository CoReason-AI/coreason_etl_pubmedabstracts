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
        raw_data -> 'MedlineCitation' as medline_citation,
        -- Extract DeleteCitation if present
        raw_data -> 'DeleteCitation' as delete_citation
    from source
),

parsed as (
    select
        file_name,
        ingestion_ts,
        content_hash,
        -- Extract PMID
        -- xmltodict structure: MedlineCitation -> PMID -> #text or directly text if no attrs
        -- We use jsonb_path_query_first or similar to handle both?
        -- Or simple ->> 'PMID' if it's simple, but it might be nested.
        -- Assuming normalized to: PMID -> {'#text': '...'} based on atomic unit 5 discovery.
        -- But sometimes it might be just text if no attrs.
        -- Safe approach: Try to get #text, coalesce with direct value.
        coalesce(
            medline_citation -> 'PMID' ->> '#text',
            medline_citation ->> 'PMID'
        ) as pmid,

        -- Extract other fields
        medline_citation -> 'Article' -> 'ArticleTitle' ->> '#text' as title,
        -- Abstract might be list of AbstractText
        medline_citation -> 'Article' -> 'Abstract' -> 'AbstractText' as abstract_json,

        -- Publication Date (Simplified)
        medline_citation -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Year' as pub_year,

        -- Authors
        medline_citation -> 'Article' -> 'AuthorList' -> 'Author' as authors_json,

        -- MeSH
        medline_citation -> 'MeshHeadingList' -> 'MeshHeading' as mesh_json

    from renamed
    where medline_citation is not null
)

select * from parsed
