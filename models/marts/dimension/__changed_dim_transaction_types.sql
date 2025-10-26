WITH cutoff_dates AS (

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_dim_transaction_types') }} AS last_cutoff_date

),

transaction_type AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_transaction_types', 
            'stg_application_transaction_types', 
            'stg_application_transaction_types_archive', 
            'ValidFrom',
            'ValidTo',
            'TransactionTypeID' 
        ) 
    }}

)

SELECT
    *
FROM 
    transaction_type