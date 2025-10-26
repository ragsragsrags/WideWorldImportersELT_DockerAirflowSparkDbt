WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_dim_customers', 'dim_customers', 'WWICustomerID')
    }}

)

SELECT
    *
FROM
    compare_result