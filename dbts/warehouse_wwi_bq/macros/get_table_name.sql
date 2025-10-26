{%- macro get_table_name(database, table) -%}
    {{ source(database, table) }}
{%- endmacro -%}
