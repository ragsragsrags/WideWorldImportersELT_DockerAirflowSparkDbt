SELECT
    TT.TransactionTypeID AS WWITransactionTypeID,
    TT.TransactionTypeName AS TransactionType
FROM
    {{ source('WideWorldImporters', 'Application_TransactionTypes') }} AS TT
WHERE
    {{ get_cutoff_date() }} BETWEEN TT.ValidFrom AND TT.ValidTo

UNION ALL

SELECT
    TTA.TransactionTypeID,
    TTA.TransactionTypeName
FROM
    {{ source('WideWorldImporters', 'Application_TransactionTypes_Archive') }} AS TTA
WHERE
    {{ get_cutoff_date() }} BETWEEN TTA.ValidFrom AND TTA.ValidTo

UNION ALL

SELECT
    0,
    'Unknown'