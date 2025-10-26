WITH stock_items AS (

    SELECT 
        *
    FROM
        {{ ref('dim_suppliers') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate