{% materialization partitioned_table, adapter='postgres' %}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(this) -%}
  {%- set partition_by = config.get('partition_by') -%}

  {%- if partition_by is none -%}
      {{ exceptions.raise_compiler_error("Model " ~ model.name ~ " is materialized as 'partitioned_table' but 'partition_by' config is missing.") }}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- 1. Create a temporary table with the data (CTAS)
  -- Standard dbt materialization pattern
  {% call statement('main') -%}
    {{ create_table_as(True, tmp_relation, sql) }}
  {%- endcall %}

  -- 2. Create the Partitioned Table Structure
  -- If exists, drop and recreate. (Full Refresh strategy for now)
  {%- if existing_relation is not none -%}
      {{ adapter.drop_relation(existing_relation) }}
  {%- endif -%}

  {% call statement('create_partitioned_table') -%}
    CREATE TABLE {{ target_relation }} (LIKE {{ tmp_relation }} INCLUDING ALL)
    PARTITION BY RANGE ({{ partition_by }});
  {%- endcall %}

  -- 3. Create Yearly Partitions (1900 to 2030)
  -- Loop to create explicit partitions for better performance and organization
  {% for year in range(1900, 2031) %}
  {%- set partition_suffix = "_" ~ year -%}
  {%- set partition_nm = target_relation.identifier ~ partition_suffix -%}
  {%- set partition_relation = target_relation.incorporate(path={"identifier": partition_nm}) -%}
  {% call statement('create_partition_' ~ year) -%}
    CREATE TABLE IF NOT EXISTS {{ partition_relation }}
    PARTITION OF {{ target_relation }}
    FOR VALUES FROM ({{ year }}) TO ({{ year + 1 }});
  {%- endcall %}
  {% endfor %}

  -- 4. Create a Default Partition
  -- Catches any data outside the explicit ranges (e.g. nulls or future dates)
  {%- set default_nm = target_relation.identifier ~ "_default" -%}
  {%- set default_relation = target_relation.incorporate(path={"identifier": default_nm}) -%}
  {% call statement('create_default_partition') -%}
    CREATE TABLE IF NOT EXISTS {{ default_relation }}
    PARTITION OF {{ target_relation }} DEFAULT;
  {%- endcall %}

  -- 5. Insert data from Temp to Target
  {% call statement('insert_data') -%}
    INSERT INTO {{ target_relation }} SELECT * FROM {{ tmp_relation }};
  {%- endcall %}

  -- 6. Create Indexes
  -- Note: Indexes on partitioned tables are automatically propagated to partitions in modern Postgres (11+)
  {{ create_indexes(target_relation) }}

  -- 7. Cleanup
  -- FIX: Explicitly set type to 'table' so drop_relation knows how to handle it
  {%- set tmp_relation = tmp_relation.incorporate(type='table') -%}
  {{ adapter.drop_relation(tmp_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
