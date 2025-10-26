WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_fct_movements', 'fct_movements', 'WWIStockItemTransactionID')
    }}

)

SELECT
    *
FROM
    compare_result