{% macro extract_pubmed_common_fields(source_alias) %}
        -- PMID
        case
            when jsonb_typeof({{ source_alias }}.raw_data -> 'MedlineCitation' -> 'PMID' -> 0) = 'string' then
                 {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'PMID' ->> 0
            else
                 {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'PMID' -> 0 ->> '#text'
        end as pmid,

        -- Title
        case
            when jsonb_typeof({{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'ArticleTitle') = 'string' then
                 {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' ->> 'ArticleTitle'
            else
                 {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'ArticleTitle' ->> '#text'
        end as title,

        -- Abstract
        case
            when jsonb_typeof({{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') = 'string' then
                 {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' ->> 'AbstractText'
            when jsonb_typeof({{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') = 'array' then
                (select string_agg(coalesce(item ->> '#text', item #>> '{}'), ' ')
                 from jsonb_array_elements({{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') as item)
            else
                 {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText' ->> '#text'
        end as abstract_text,

        -- Publication Year
        coalesce(
            {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Year',
            substring({{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'MedlineDate' from '\d{4}')
        ) as pub_year,

        -- Date Components
        {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Month' as pub_month,
        {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Day' as pub_day,
        {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'MedlineDate' as medline_date,

        -- Authors
        {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'AuthorList' -> 'Author' as authors,

        -- MeSH
        {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'MeshHeadingList' -> 'MeshHeading' as mesh_terms,

        -- Language
        {{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'Language' as languages,

        -- DOI Extraction
        (
            select item ->> '#text'
            from jsonb_array_elements(coalesce({{ source_alias }}.raw_data -> 'MedlineCitation' -> 'Article' -> 'ELocationID', '[]'::jsonb)) as item
            where item ->> '@EIdType' = 'doi'
            limit 1
        ) as doi
{% endmacro %}
