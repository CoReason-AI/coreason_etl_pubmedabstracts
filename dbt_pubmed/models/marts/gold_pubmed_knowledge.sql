{{
    config(
        materialized='table',
        schema='gold',
        alias='pubmed_abstract_knowledge'
    )
}}

with source as (
    select * from {{ ref('int_pubmed_deduped') }}
),

final as (
    select
        s.pmid,
        s.coreason_id,
        s.title,
        s.doi,
        s.abstract_text,
        
        -- Primary Date Column as requested
        s.publication_date,

        -- Added Metadata Columns
        s.publication_status,
        
        -- Formatted Publication Types (extracting list of strings)
        (
            select jsonb_agg(
                case 
                    when jsonb_typeof(pt) = 'string' then pt #>> '{}'
                    when jsonb_typeof(pt) = 'object' then pt ->> '#text'
                    else null
                end
            )
            from jsonb_array_elements(
                case 
                    when jsonb_typeof(s.publication_type_list) = 'array' then s.publication_type_list
                    else jsonb_build_array(s.publication_type_list)
                end
            ) as pt
        ) as publication_types,
        
        s.medline_ta as "Journal_Tittle_abbreviation",
        s.journal_volume,
        s.journal_issue,
        s.article_pagination_medlinepgn as "Article_pagination",

        -- FORMATTED AUTHORS
        (
            select jsonb_agg(
                jsonb_build_object(
                    'initials',    a ->> 'Initials',
                    'fore_name',   a ->> 'ForeName',
                    'last_name',   a ->> 'LastName',
                    'affiliation', case
                        when jsonb_typeof(a -> 'AffiliationInfo') = 'array' then a -> 'AffiliationInfo' -> 0 ->> 'Affiliation'
                        when jsonb_typeof(a -> 'AffiliationInfo') = 'object' then a -> 'AffiliationInfo' ->> 'Affiliation'
                        else null
                    end
                )
            )
            from jsonb_array_elements(coalesce(s.authors, '[]'::jsonb)) as a
        ) as authors,

        -- FORMATTED MESH
        (
            select jsonb_agg(
                jsonb_build_object(
                    'qualifiers', m -> 'QualifierName',
                    
                    'descriptor_ui', case 
                        when jsonb_typeof(m -> 'DescriptorName') = 'object' then m -> 'DescriptorName' ->> '@UI' 
                        else null 
                    end,
                    
                    'descriptor_name', case 
                        when jsonb_typeof(m -> 'DescriptorName') = 'object' then m -> 'DescriptorName' ->> '#text' 
                        else m ->> 'DescriptorName' 
                    end
                )
            )
            from jsonb_array_elements(coalesce(s.mesh_terms, '[]'::jsonb)) as m
        ) as mesh_terms,

        s.languages,
        s.file_name,
        s.ingestion_ts

    from source s
)

select *
from final
where abstract_text is not null
and (
    exists (
        select 1
        from jsonb_array_elements_text(coalesce(languages, '[]'::jsonb)) as lang
        where lang = '{{ var("FILTER_LANGUAGE", "eng") }}'
    )
    OR '{{ var("FILTER_LANGUAGE", "eng") }}' = ''
    OR '{{ var("FILTER_LANGUAGE", "eng") }}' IS NULL
)
