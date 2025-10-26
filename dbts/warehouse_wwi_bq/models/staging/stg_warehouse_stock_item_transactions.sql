WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Warehouse_StockItemTransactions') }}

),

transformed AS (

    SELECT
        CAST(TransactionOccurredWhen AS DATE) AS TransactionOccurredWhen,
        StockItemTransactionID,
		InvoiceID,
		PurchaseOrderID,
		Quantity,
		StockItemID,
        CustomerID,
        SupplierID,
        TransactionTypeID,
        LastEditedWhen
    FROM
        source

)

SELECT
    *
FROM
    transformed