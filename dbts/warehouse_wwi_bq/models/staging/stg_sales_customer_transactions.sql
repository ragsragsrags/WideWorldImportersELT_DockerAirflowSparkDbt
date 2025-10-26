WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_CustomerTransactions') }}

),

transformed AS (

    SELECT
        CustomerTransactionID,
        CustomerID,
        TransactionTypeID,
        InvoiceID,
        PaymentMethodID,
        CAST(TransactionDate AS DATE) AS TransactionDate,
        AmountExcludingTax AS TotalExcludingTax,
        TaxAmount,
        TransactionAmount AS TotalIncludingTax,
        OutstandingBalance,
        FinalizationDate,
        IsFinalized,
        LastEditedBy,
        LastEditedWhen
    FROM
        source

)

SELECT
    *
FROM
    transformed