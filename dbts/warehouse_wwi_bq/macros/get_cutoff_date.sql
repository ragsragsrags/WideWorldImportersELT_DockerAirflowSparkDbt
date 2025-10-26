{%- macro get_cutoff_date() -%}
    {% set sql_statement_b %}
        SELECT
            cutoff_date
        FROM
            {{ source('WideWorldImportersDW', 'LatestCutoffDate') }}
    {% endset %}
    {% set cutoff_date = dbt_utils.get_single_value(sql_statement_b) %}

    cast('{{ cutoff_date }}' as datetime)
{%- endmacro -%}
