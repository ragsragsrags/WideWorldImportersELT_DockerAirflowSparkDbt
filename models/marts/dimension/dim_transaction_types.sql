WITH transaction_types AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_dim_transaction_types') }}      

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY transaction_types.TransactionTypeID) AS TransactionTypeKey,
        transaction_types.TransactionTypeID AS WWITransactionTypeID,
        transaction_types.TransactionTypeName AS TransactionType
    FROM
        transaction_types

    UNION ALL

    SELECT
        0,
        0,
        'Unknown'

)

SELECT 
    * 
FROM 
    final
ORDER BY
    TransactionTypeKey
