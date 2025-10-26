{%- macro get_table_info(database, table, type) -%}
    {%- if type == "database" -%}
        {%- set sql_statement_database -%}
        SELECT 
            REPLACE(SPLIT('{{ source(database, table) }}', '.')[ORDINAL(1)], '`', '');
        {%- endset -%}
        {%- set name = dbt_utils.get_single_value(sql_statement_database) -%}
    {%- elif type == "schema" -%}
        {%- set sql_statement_database -%}
        SELECT 
            REPLACE(SPLIT('{{ source(database, table) }}', '.')[ORDINAL(2)], '`', '');
        {%- endset -%}
        {%- set name = dbt_utils.get_single_value(sql_statement_database) -%}
    {%- else -%}
        {%- set sql_statement_database -%}
        SELECT 
            REPLACE(SPLIT('{{ source(database, table) }}', '.')[ORDINAL(3)], '`', '');
        {%- endset -%}
        {%- set name = dbt_utils.get_single_value(sql_statement_database) -%}
    {%- endif -%}
    {{ name }}
{%- endmacro -%}