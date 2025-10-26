WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_dim_payment_methods', 'dim_payment_methods', 'WWIPaymentMethodID')
    }}

)

SELECT
    *
FROM
    compare_result