WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_fct_orders', 'fct_orders', ['WWIOrderID', 'WWIOrderLineID'])
    }}

)

SELECT
    *
FROM
    compare_result