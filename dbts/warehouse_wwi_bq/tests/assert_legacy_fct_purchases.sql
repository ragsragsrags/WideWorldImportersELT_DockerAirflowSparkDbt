WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_fct_purchases', 'fct_purchases', ['WWIPurchaseOrderID', 'WWIPurchaseOrderLineID'])
    }}

)

SELECT
    *
FROM
    compare_result
