WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Purchasing_SupplierTransactions') }}

),

transformed AS (

    SELECT
        SupplierTransactionID,
        SupplierID,
        TransactionTypeID,
        PurchaseOrderID,
        PaymentMethodID,
        SupplierInvoiceNumber,
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