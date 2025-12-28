{% materialization partitioned_table, adapter='postgres' %}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(this) -%}
  {%- set partition_by = config.get('partition_by') -%}

  {%- if partition_by is none -%}
      {{ exceptions.raise_compiler_error("Model " ~ model.name ~ " is materialized as 'partitioned_table' but 'partition_by' config is missing.") }}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  --------------------------------------------------------------------------------------------------------------------
  -- 1. Create a temporary table with the data
  --------------------------------------------------------------------------------------------------------------------
  {% call statement('main') -%}
    {{ create_table_as(True, tmp_relation, sql) }}
  {%- endcall %}

  --------------------------------------------------------------------------------------------------------------------
  -- 2. Create the Partitioned Table Structure
  --------------------------------------------------------------------------------------------------------------------
  -- Drop the existing target relation if it exists
  {%- if existing_relation is not none -%}
      {{ adapter.drop_relation(existing_relation) }}
  {%- endif -%}

  {% call statement('create_partitioned_table') -%}
    CREATE TABLE {{ target_relation }} (LIKE {{ tmp_relation }} INCLUDING ALL)
    PARTITION BY RANGE ({{ partition_by }});
  {%- endcall %}

  --------------------------------------------------------------------------------------------------------------------
  -- 3. Create a Default Partition
  -- Essential because Postgres will fail the INSERT if rows don't map to a partition.
  --------------------------------------------------------------------------------------------------------------------
  {% call statement('create_default_partition') -%}
    CREATE TABLE {{ target_relation }}_default
    PARTITION OF {{ target_relation }} DEFAULT;
  {%- endcall %}

  --------------------------------------------------------------------------------------------------------------------
  -- 4. Insert data from Temp to Target
  --------------------------------------------------------------------------------------------------------------------
  {% call statement('insert_data') -%}
    INSERT INTO {{ target_relation }} SELECT * FROM {{ tmp_relation }};
  {%- endcall %}

  --------------------------------------------------------------------------------------------------------------------
  -- 5. Create Indexes
  -- Explicitly creating indexes defined in model config
  --------------------------------------------------------------------------------------------------------------------
  {{ create_indexes(target_relation) }}

  --------------------------------------------------------------------------------------------------------------------
  -- 6. Cleanup
  --------------------------------------------------------------------------------------------------------------------
  {{ adapter.drop_relation(tmp_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
