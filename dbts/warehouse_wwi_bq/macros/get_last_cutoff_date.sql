{%- macro get_last_cutoff_date(database, table) -%}
    {% set sql_statement_database %}
        SELECT 
            REPLACE(SPLIT('{{ source(database, table) }}', '.')[ORDINAL(1)], '`', '')
    {% endset %}
    {% set table_database = dbt_utils.get_single_value(sql_statement_database) %}

    {% set sql_statement_schema %}
        SELECT 
             REPLACE(SPLIT('{{ source(database, table) }}', '.')[ORDINAL(2)], '`', '')
    {% endset %}
    {% set table_schema = dbt_utils.get_single_value(sql_statement_schema) %}

    {% set sql_statement_table %}
        SELECT 
             REPLACE(SPLIT('{{ source(database, table) }}', '.')[ORDINAL(3)], '`', '')
    {% endset %}
    {% set table_name = dbt_utils.get_single_value(sql_statement_table) %}

    {% set sql_statement %}
        
        SELECT 
            EXISTS(
                SELECT 
                    1 
                FROM 
                    `{{ table_database }}`.`{{ table_schema }}`.`INFORMATION_SCHEMA`.`TABLES`
                WHERE 
                    table_name LIKE '{{ table_name }}' AND
                    table_schema LIKE '{{ table_schema }}'
            )
    
    {% endset %}

    {%- set tables_exists = dbt_utils.get_single_value(sql_statement) -%}

    {% set sql_statement_a %}
    {% if tables_exists %}
        SELECT
            LoadDate AS last_cutoff_date
        FROM
            `{{ table_database }}`.`{{ table_schema }}`.`{{ table_name }}`
    {% else %}
        SELECT 
            CAST({{ "'" + var('initial_load') + "'" }} AS DATETIME) AS last_cutoff_date 
    {% endif %}
    {% endset %}
    {% set last_cutoff_date = dbt_utils.get_single_value(sql_statement_a) %}

    CAST('{{ last_cutoff_date }}' AS DATETIME)

{%- endmacro -%}