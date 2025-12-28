{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ ref('int_pubmed_deduped') }}
),

flattened_mesh as (
    select
        source_id,
        coreason_id,

        -- Explode mesh_terms array
        jsonb_array_elements(mesh_terms) as mesh_json

    from source
    where mesh_terms is not null
    and jsonb_typeof(mesh_terms) = 'array'
),

final as (
    select
        coreason_id,
        source_id,

        -- Generate deterministic MeSH ID
        -- Priority: Descriptor UI (Stable) > Descriptor Name (Fallback)
        -- Using DNS namespace: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
        uuid_generate_v5(
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'::uuid,
            coalesce(
                case
                    when jsonb_typeof(mesh_json -> 'DescriptorName') = 'object' then
                        mesh_json -> 'DescriptorName' ->> '@UI'
                    else null
                end,
                case
                    when jsonb_typeof(mesh_json -> 'DescriptorName') = 'object' then
                        mesh_json -> 'DescriptorName' ->> '#text'
                    else
                        mesh_json ->> 'DescriptorName'
                end
            )
        ) as mesh_id,

        -- DescriptorName
        case
             when jsonb_typeof(mesh_json -> 'DescriptorName') = 'object' then
                  mesh_json -> 'DescriptorName' ->> '#text'
             else
                  mesh_json ->> 'DescriptorName'
        end as descriptor_name,

        -- Descriptor UI (Unique Identifier)
        case
             when jsonb_typeof(mesh_json -> 'DescriptorName') = 'object' then
                  mesh_json -> 'DescriptorName' ->> '@UI'
             else null
        end as descriptor_ui,

        -- QualifierName (Can be a list or single object)
        -- For simplicity, we might extract the first qualifier if multiple,
        -- or we'd need another lateral join.
        -- Given the Star Schema request (1-row-per-term), we treat (Descriptor, Qualifier) as the grain?
        -- Or just Descriptor?
        -- Usually MeSH is Descriptor + List of Qualifiers.
        -- Let's extract QualifierName as a JSON array or string for now to avoid N*M*K explosion here.
        mesh_json -> 'QualifierName' as qualifier_json

    from flattened_mesh
)

select * from final
