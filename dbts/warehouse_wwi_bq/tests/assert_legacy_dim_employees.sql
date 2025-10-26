WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_dim_employees', 'dim_employees', 'WwIEmployeeID')
    }}

)

SELECT
    *
FROM
    compare_result