WITH movements AS (

    SELECT
        SIT.TransactionOccurredWhen,
        SI.StockItemID,
        SI.StockItemName,
        C.CustomerID,
        C.CustomerName,
        S.SupplierID,
        S.SupplierName,
        TT.TransactionTypeID,
        TT.TransactionTypeName,
        SIT.StockItemTransactionID,
        SIT.InvoiceID,
        SIT.PurchaseOrderID,
        SIT.Quantity
    FROM
        {{ source('WideWorldImporters', 'Warehouse_StockItemTransactions') }} AS SIT LEFT JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName
            FROM
                {{ source('WideWorldImporters', 'Warehouse_StockItems') }} AS SI 
            WHERE
                {{ get_cutoff_date() }} BETWEEN Si.ValidFrom AND SI.ValidTo

            UNION ALL

            SELECT
                SIA.StockItemID,
                SIA.StockItemName
            FROM
                {{ source('WideWorldImporters', 'Warehouse_StockItems_Archive') }} AS SIA
            WHERE
                {{ get_cutoff_date() }} BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = SIT.StockItemID LEFT JOIN
        (
            SELECT
                C.CustomerID,
                C.CustomerName
            FROM
                {{ source('WideWorldImporters', 'Sales_Customers') }} AS C 
            WHERE
                {{ get_cutoff_date() }} BETWEEN C.ValidFrom AND C.ValidTo

            UNION ALL

            SELECT
                CA.CustomerID,
                CA.CustomerName
            FROM
                {{ source('WideWorldImporters', 'Sales_Customers_Archive') }} AS CA
            WHERE
                {{ get_cutoff_date() }} BETWEEN CA.ValidFrom AND CA.ValidTo
        ) C ON 
            C.CustomerID = SIT.CustomerID LEFT JOIN
        (
            SELECT
                S.SupplierID,
                S.SupplierName
            FROM
                {{ source('WideWorldImporters', 'Purchasing_Suppliers') }} AS S 
            WHERE
                {{ get_cutoff_date() }} BETWEEN S.ValidFrom AND S.ValidTo

            UNION ALL

            SELECT
                SA.SupplierID,
                SA.SupplierName
            FROM
                {{ source('WideWorldImporters', 'Purchasing_Suppliers_Archive') }} AS SA
            WHERE
                {{ get_cutoff_date() }} BETWEEN SA.ValidFrom AND SA.ValidTo
        ) S ON 
            S.SupplierID = SIT.SupplierID LEFT JOIN
        (
            SELECT
                TT.TransactionTypeID,
                TT.TransactionTypeName
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
        ) TT ON 
            TT.TransactionTypeID = SIT.TransactionTypeID
    WHERE
        SIT.LastEditedWhen <= {{ get_cutoff_date() }}

),

final AS (

    SELECT
        *
    FROM
        movements

)

SELECT 
    CAST(final.TransactionOccurredWhen AS DATE) AS DateKey,
    final.StockItemTransactionID AS WWIStockItemTransactionID,
    final.StockItemID AS WWIStockItemID,
    final.CustomerID AS WWICustomerID,
    final.SupplierID AS WWISupplierID,
    final.TransactionTypeID AS WWITransactionTypeID,
    final.InvoiceID AS WWIInvoiceID,
    final.PurchaseOrderID AS WWIPurchaseOrderID,
    final.quantity AS Quantity
FROM
    final