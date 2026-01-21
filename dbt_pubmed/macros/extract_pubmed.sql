{% macro extract_pubmed_common_fields(source_alias) %}
        -- PMID (Integer)
        case
            when jsonb_typeof({{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'PMID' -> 0) = 'string' then
                 {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'PMID' ->> 0
            else
                 {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'PMID' -> 0 ->> '#text'
        end as pmid,

        -- Title
        case
            when jsonb_typeof({{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'ArticleTitle') = 'string' then
                 {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' ->> 'ArticleTitle'
            else
                 {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'ArticleTitle' ->> '#text'
        end as title,

        -- Abstract
        case
            when jsonb_typeof({{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') = 'string' then
                 {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Abstract' ->> 'AbstractText'
            when jsonb_typeof({{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') = 'array' then
                (select string_agg(coalesce(item ->> '#text', item #>> '{}'), ' ')
                 from jsonb_array_elements({{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText') as item)
            else
                 {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Abstract' -> 'AbstractText' ->> '#text'
        end as abstract_text,

        -- Publication Year
        coalesce(
            {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Year',
            substring({{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'MedlineDate' from '\d{4}')
        ) as pub_year,

        -- Date Components
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Month' as pub_month,
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'Day' as pub_day,
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' -> 'PubDate' ->> 'MedlineDate' as medline_date,

        -- NEW COLUMNS ---------------------------------------------------------
        -- Publication Status
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'PubmedData' ->> 'PublicationStatus' as publication_status,

        -- Publication Type (Extract as JSONB to preserve list structure)
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'PublicationTypeList' -> 'PublicationType' as publication_type_list,

        -- Medline TA
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'MedlineJournalInfo' ->> 'MedlineTA' as medline_ta,

        -- Journal Volume
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' ->> 'Volume' as journal_volume,

        -- Journal Issue
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Journal' -> 'JournalIssue' ->> 'Issue' as journal_issue,

        -- Pagination (MedlinePgn)
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Pagination' ->> 'MedlinePgn' as article_pagination_medlinepgn,
        ------------------------------------------------------------------------

        -- Authors
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'AuthorList' -> 'Author' as authors,

        -- MeSH
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'MeshHeadingList' -> 'MeshHeading' as mesh_terms,

        -- Language
        {{ source_alias }}.raw_data -> 'PubmedArticle' -> 'MedlineCitation' -> 'Article' -> 'Language' as languages,

        -- DOI Extraction
        (
            select item ->> '#text'
            from jsonb_array_elements(coalesce({{ source_alias }}.raw_data -> 'PubmedArticle' -> 'PubmedData' -> 'ArticleIdList' -> 'ArticleId', '[]'::jsonb)) as item
            where item ->> '@IdType' = 'doi'
            limit 1
        ) as doi
{% endmacro %}
