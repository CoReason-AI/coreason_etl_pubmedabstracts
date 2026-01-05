{{
    config(
        materialized='incremental',
        unique_key='source_id',
        pre_hook="""
            {% if is_incremental() %}
                -- Create watermark table only if incremental and the target table exists
                -- We use a safe strategy: capture max_ts if table exists, else 0.
                -- Note: '{{ this }}' resolution depends on existence.
                create temporary table if not exists pubmed_deduped_watermark as
                select coalesce(max(ingestion_ts), 0) as max_ts
                from {{ this }};
            {% endif %}
        """,
        post_hook="""
            {% if is_incremental() %}
                -- Physical Hard Delete Implementation
                -- Requirement: Retracted papers (operation='delete') must be removed from the dataset.
                -- Strategy: Delete from the target table where a corresponding 'delete' record
                -- exists in the current batch (newer than the captured watermark).
                delete from {{ this }}
                where source_id in (
                    select pmid
                    from (
                        select
                            pmid,
                            operation,
                            -- Rank to find the latest operation for this PMID in the batch
                            row_number() over (partition by pmid order by file_name desc, ingestion_ts desc) as rn
                        from {{ ref('stg_pubmed_citations') }}
                        where ingestion_ts > (select max_ts from pubmed_deduped_watermark)
                    ) s
                    where rn = 1 and operation = 'delete'
                );

                -- Cleanup
                drop table if exists pubmed_deduped_watermark;
            {% endif %}
        """
    )
}}

with source_data as (
    select * from {{ ref('stg_pubmed_citations') }}
    {% if is_incremental() %}
    -- Optimization: Only scan new records
    where ingestion_ts > (select max_ts from pubmed_deduped_watermark)
    {% endif %}
),

ranked as (
    select
        *,
        -- Rank by file_name (alphanumeric sort) to determine the latest state.
        -- Files are named like pubmed24n0001.xml.gz, pubmed24n1001.xml.gz
        -- Higher number = later.
        row_number() over (partition by pmid order by file_name desc, ingestion_ts desc) as rn
    from source_data
    where pmid is not null
),

final as (
    select
        pmid as source_id,
        -- Generate deterministic UUID
        uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid,
            pmid
        ) as coreason_id,

        -- Pass through extracted columns
        title,
        doi,
        abstract_text,
        pub_year,

        -- Construct Publication Date
        make_date(
            -- Year: Use extracted pub_year or regex from medline_date, fallback to 1900
            coalesce(
                nullif(pub_year, '')::int,
                (substring(medline_date from '\d{4}'))::int,
                1900
            ),
            -- Month: Map text to int, fallback to 1
            case
                -- Handle numeric months first
                when pub_month ~ '^\d+$' then (pub_month)::int
                -- Handle short text
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
                -- Default
                else 1
            end,
            -- Day: Use extracted day or fallback to 1
            case
                when pub_day ~ '^\d+$' then (pub_day)::int
                else 1
            end
        ) as publication_date,

        authors,
        mesh_terms,
        languages,

        raw_data,
        file_name,
        ingestion_ts
    from ranked
    where rn = 1
    and operation = 'upsert' -- Only keep if the latest operation is an upsert
)

select * from final
