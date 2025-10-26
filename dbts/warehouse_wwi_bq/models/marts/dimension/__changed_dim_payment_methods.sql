WITH payment_methods AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_payment_methods', 
            'stg_application_payment_methods', 
            'stg_application_payment_methods_archive', 
            'ValidFrom',
            'ValidTo',
            'PaymentMethodID' 
        ) 
    }}

)

SELECT
    *
FROM 
    payment_methods