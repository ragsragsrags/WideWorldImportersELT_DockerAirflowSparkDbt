WITH source AS
(

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Application_TransactionTypes_Archive') }}

),

transformed AS (

    SELECT
        TransactionTypeID,
        TransactionTypeName,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    *
FROM
    transformed