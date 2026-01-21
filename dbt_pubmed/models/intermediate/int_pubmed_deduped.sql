{% set post_hook_sql %}
    {% if is_incremental() %}
        -- Physical Hard Delete Implementation
        delete from {{ this }}
        where pmid in (
            select pmid
            from (
                select
                    pmid,
                    operation,
                    row_number() over (partition by pmid order by file_name desc, ingestion_ts desc) as rn
                from {{ ref('stg_pubmed_citations') }}
                where ingestion_ts > (select coalesce(max(ingestion_ts), 0) from {{ this }})
            ) s
            where rn = 1 and operation = 'delete'
        );
    {% endif %}
{% endset %}

{{
    config(
        materialized='incremental',
        schema='silver',
        alias='pubmed_abstract_citations',
        unique_key='pmid',
        post_hook=post_hook_sql
    )
}}

with source_data as (
    select * from {{ ref('stg_pubmed_citations') }}
    {% if is_incremental() %}
    where ingestion_ts > (select coalesce(max(ingestion_ts), 0) from {{ this }})
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (partition by pmid order by file_name desc, ingestion_ts desc) as rn
    from source_data
    where pmid is not null
),

final as (
    select
        pmid::int as pmid,
        
        uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid,
            pmid
        ) as coreason_id,

        title,
        doi,
        abstract_text,
        pub_year,

        make_date(
            coalesce(
                nullif(pub_year, '')::int,
                (substring(medline_date from '\d{4}'))::int,
                1900
            ),
            case
                when pub_month ~ '^\d+$' then (pub_month)::int
                when lower(pub_month) in ('jan', 'january', '01') then 1
                when lower(pub_month) in ('feb', 'february', '02') then 2
                when lower(pub_month) in ('mar', 'march', '03') then 3
                when lower(pub_month) in ('apr', 'april', '04') then 4
                when lower(pub_month) in ('may', '05') then 5
                when lower(pub_month) in ('jun', 'june', '06') then 6
                when lower(pub_month) in ('jul', 'july', '07') then 7
                when lower(pub_month) in ('aug', 'august', '08') then 8
                when lower(pub_month) in ('sep', 'september', '09') then 9
                when lower(pub_month) in ('oct', 'october', '10') then 10
                when lower(pub_month) in ('nov', 'november', '11') then 11
                when lower(pub_month) in ('dec', 'december', '12') then 12
                else 1
            end,
            case
                when pub_day ~ '^\d+$' then (pub_day)::int
                else 1
            end
        ) as publication_date,

        -- New Columns
        publication_status,
        publication_type_list,
        medline_ta,
        journal_volume,
        journal_issue,
        article_pagination_medlinepgn,
        --

        authors,
        mesh_terms,
        languages,

        raw_data,
        file_name,
        ingestion_ts
    from ranked
    where rn = 1
    and operation = 'upsert'
)

select * from final
