WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_dim_suppliers', 'dim_suppliers', 'WWISupplierID')
    }}

)

SELECT
    *
FROM
    compare_result