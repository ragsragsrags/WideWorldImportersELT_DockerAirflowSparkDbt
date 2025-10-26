WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_fct_stock_holdings', 'fct_stock_holdings', 'WWIStockItemID')
    }}

)

SELECT
    *
FROM
    compare_result
