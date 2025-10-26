WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_dim_stock_items', 'dim_stock_items', 'WWIStockItemID')
    }}

)

SELECT
    *
FROM
    compare_result