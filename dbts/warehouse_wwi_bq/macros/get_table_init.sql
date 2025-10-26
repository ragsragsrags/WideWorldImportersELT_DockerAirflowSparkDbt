{%- macro get_table_init(changedTable, initTable, view, key) -%}
    WITH changed AS (
    
        SELECT
            *
        FROM
            {{ ref(changedTable) }}

    ),

    init AS (
        
        SELECT
            *
        FROM
            {{ 
                table_exists_by_stage_view
                (
                    "WideWorldImportersDW", 
                    initTable, 
                    view
                ) 
            }}

    ),

    final AS (

        SELECT
            *
        FROM
            changed

        UNION ALL

        SELECT
            *
        FROM
            init
        WHERE
            init.{{ key }} NOT IN  (
                SELECT
                    changed.{{ key }}
                FROM
                    changed
            ) 

    )

    SELECT 
        * 
    FROM 
        final

{%- endmacro -%}