WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_dim_transaction_types', 'dim_transaction_types', 'WWITransactionTypeID')
    }}

)

SELECT
    *
FROM
    compare_result