WITH compare_result AS (

    {{ 
        test_existing_with_legacy('legacy_fct_sales', 'fct_sales', ['WWIInvoiceID', 'WWIInvoiceLineID'])
    }}

)

SELECT
    *
FROM
    compare_result
