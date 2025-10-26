WITH stock_items AS (

    SELECT 
        *
    FROM
        {{ ref('dim_stock_items') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate