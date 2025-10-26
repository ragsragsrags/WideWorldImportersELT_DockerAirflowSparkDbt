WITH transactions AS (

    SELECT
        CT.TransactionDate AS DateKey,
        CT.CustomerTransactionID AS WWICustomerTransactionID,
        CAST(NULL AS INTEGER) AS WWISupplierTransactionID,
        C.CustomerID AS WWICustomerID,
        BC.CustomerID AS WWIBillToCustomerID,
        CAST(NULL AS INTEGER) AS WWISupplierID,
        TT.TransactionTypeID AS WWITransactionTypeID,
        PM.PaymentMethodID AS WWIPaymentMethodID,
        CT.InvoiceID AS WWIInvoiceID,
        CAST(NULL AS INTEGER) AS WWIPurchaseOrderID,
        CAST(NULL AS STRING) AS SupplierInvoiceNumber,
        CT.AmountExcludingTax AS TotalExcludingTax,
        CT.TaxAmount,
        CT.TransactionAmount AS TotalIncludingTax,
        CT.OutstandingBalance,
        CT.IsFinalized
    FROM
        {{ source('WideWorldImporters', 'Sales_CustomerTransactions') }} AS CT LEFT JOIN
        {{ source('WideWorldImporters', 'Sales_Invoices') }} AS I ON
            I.InvoiceID = CT.InvoiceID LEFT JOIN
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
            C.CustomerID = COALESCE(I.CustomerID, CT.CustomerID) LEFT JOIN
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
        ) BC ON
            BC.CustomerID = CT.CustomerID LEFT JOIN
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
                {{ source('WideWorldImporters', 'Application_TransactionTypes_Archive') }} AS  TTA 
            WHERE
                {{ get_cutoff_date() }} BETWEEN TTA.ValidFrom AND TTA.ValidTo
        ) TT ON
            TT.TransactionTypeID = CT.TransactionTypeID LEFT JOIN
        (
            SELECT
                PM.PaymentMethodID,
                PM.PaymentMethodName
            FROM
                {{ source('WideWorldImporters', 'Application_PaymentMethods') }} AS PM 
            WHERE
                {{ get_cutoff_date() }} BETWEEN PM.ValidFrom AND PM.ValidTo

            UNION ALL

            SELECT
                PMA.PaymentMethodID,
                PMA.PaymentMethodName
            FROM
                {{ source('WideWorldImporters', 'Application_PaymentMethods_Archive') }} AS PMA
            WHERE
                {{ get_cutoff_date() }} BETWEEN PMA.ValidFrom AND PMA.ValidTo
        ) PM ON
            PM.PaymentMethodID = CT.PaymentMethodID
    WHERE
        CT.LastEditedWhen <= {{ get_cutoff_date() }}

    UNION ALL

    SELECT
        ST.TransactionDate,
        CAST(NULL AS INTEGER),
        ST.SupplierTransactionID,
        CAST(NULL AS INTEGER),
        CAST(NULL AS INTEGER),
        S.SupplierID,
        TT.TransactionTypeID,
        PM.PaymentMethodID,
        CAST(NULL AS INTEGER),
        ST.PurchaseOrderID,
        ST.SupplierInvoiceNumber,
        ST.AmountExcludingTax,
        ST.TaxAmount,
        ST.TransactionAmount,
        ST.OutstandingBalance,
        ST.IsFinalized
    FROM
        {{ source('WideWorldImporters', 'Purchasing_SupplierTransactions') }} AS ST LEFT JOIN
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
            S.SupplierID = ST.SupplierID LEFT JOIN
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
            TT.TransactionTypeID = ST.TransactionTypeID LEFT JOIN
        (
            SELECT
                PM.PaymentMethodID,
                PM.PaymentMethodName
            FROM
                {{ source('WideWorldImporters', 'Application_PaymentMethods') }} AS PM 
            WHERE
                {{ get_cutoff_date() }} BETWEEN PM.ValidFrom AND PM.ValidTo

            UNION ALL

            SELECT
                PMA.PaymentMethodID,
                PMA.PaymentMethodName
            FROM
                {{ source('WideWorldImporters', 'Application_PaymentMethods_Archive') }} AS  PMA
            WHERE
                {{ get_cutoff_date() }} BETWEEN PMA.ValidFrom AND PMA.ValidTo
        ) PM ON
            PM.PaymentMethodID = ST.PaymentMethodID
    WHERE
        ST.LastEditedWhen <= {{ get_cutoff_date() }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                DateKey,
                WWICustomerTransactionID,
                WWISupplierTransactionID
        ) AS TransactionKey,
        *
    FROM
        transactions
)

SELECT
    *
FROM
    final