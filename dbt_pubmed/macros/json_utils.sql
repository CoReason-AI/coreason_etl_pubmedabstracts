{% macro normalize_json_list(json_column) %}
    case
        when jsonb_typeof({{ json_column }}) = 'array' then {{ json_column }}
        when {{ json_column }} is null then '[]'::jsonb
        else jsonb_build_array({{ json_column }})
    end
{% endmacro %}
