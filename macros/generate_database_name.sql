{% macro generate_database_name(custom_database_name, node) -%}

    {%- set env_prefix = var('env') | upper -%}

    {%- if custom_database_name is none -%}
        {{ target.database }}

    {%- else -%}
        {{ env_prefix }}_{{ custom_database_name | trim | upper }}_YGO_DB

    {%- endif -%}

{%- endmacro %} 