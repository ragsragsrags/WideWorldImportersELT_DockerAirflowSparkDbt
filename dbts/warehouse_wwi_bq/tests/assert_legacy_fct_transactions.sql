WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_fct_transactions', 'fct_transactions', 'TransactionKey')
    }}

)

SELECT
    *
FROM
    compare_result
