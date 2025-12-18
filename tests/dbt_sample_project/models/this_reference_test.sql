-- Model to test {{ this }} support in dbt-toolbox
-- Uses {{ this }} to reference the current model's relation

{{ config(materialized="table") }}

-- Check if the table exists using {{ this }}
{%- set relation = adapter.get_relation(this.database, this.schema, this.table) %}

-- Get existing columns if table exists
{%- if relation is not none -%}
    {%- set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list -%}
{%- else -%}
    {%- set existing_columns = [] -%}
{%- endif %}

-- Build the query
select
    '{{ this.database }}' as current_database,
    '{{ this.schema }}' as current_schema,
    '{{ this.table }}' as current_table,
    '{{ this }}' as full_relation,
    {% if existing_columns | length > 0 %}
        '{{ existing_columns | join(", ") }}' as existing_columns
    {% else %}
        'No existing columns' as existing_columns
    {% endif %}
