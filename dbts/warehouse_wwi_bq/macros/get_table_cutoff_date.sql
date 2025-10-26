{%- macro get_table_cutoff_date(table) -%}
     WITH table_top1 AS (

        SELECT 
            *
        FROM
            {{ ref(table) }}
        LIMIT 1

    )

    SELECT
        {{ get_cutoff_date() }}  AS LoadDate

{%- endmacro -%}