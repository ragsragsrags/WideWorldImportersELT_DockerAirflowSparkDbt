WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_dim_cities', 'dim_cities', 'WWICityID')
    }}

)

SELECT
    *
FROM
    compare_result