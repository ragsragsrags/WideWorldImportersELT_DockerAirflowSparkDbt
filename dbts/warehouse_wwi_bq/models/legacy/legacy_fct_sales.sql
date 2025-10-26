WITH sales AS (
    SELECT
        CAST(I.InvoiceDate AS DATE) AS InvoiceDateKey,
        CAST(I.ConfirmedDeliveryTime AS DATE) AS DeliveryDateKey,
        I.InvoiceID AS WWIInvoiceID,
        IL.InvoiceLineID AS WWIInvoiceLineID,
        C.CityID AS WWICityID,
        CU.CustomerID AS WWICustomerID,
        BCU.CustomerID AS WWIBillToCustomerID,
        SI.StockItemID AS WWIStockItemID,
        I.SalespersonPersonID AS WWISalesPersonID,
        IL.Description,
        PT.PackageTypeName AS Package,
        IL.Quantity,
        IL.UnitPrice,
        IL.TaxRate,
        IL.ExtendedPrice - IL.TaxAmount AS TotalExcludingTax,
        IL.TaxAmount AS TaxAmount,
        IL.LineProfit AS Profit,
        IL.ExtendedPrice AS TotalIncludingTax,
        CASE 
            WHEN SI.IsChillerStock = FALSE THEN IL.Quantity 
            ELSE 0 
        END AS TotalDryItems,
        CASE 
            WHEN SI.IsChillerStock <> FALSE THEN IL.Quantity 
            ELSE 0 
        END AS TotalChillerItems
    FROM
        {{ source('WideWorldImporters', 'Sales_Invoices') }} AS I LEFT JOIN
        {{ source('WideWorldImporters', 'Sales_InvoiceLines') }} AS IL ON
            IL.InvoiceID = I.InvoiceID LEFT JOIN
        (
            SELECT
                C.CustomerID,
                C.CustomerName,
                C.DeliveryCityID
            FROM
                {{ source('WideWorldImporters', 'Sales_Customers') }} AS C
            WHERE
                {{ get_cutoff_date() }} BETWEEN C.ValidFrom AND C.ValidTo

            UNION ALL

            SELECT
                CA.CustomerID,
                CA.CustomerName,
                CA.DeliveryCityID
            FROM
                {{ source('WideWorldImporters', 'Sales_Customers_Archive') }} AS CA
            WHERE
                {{ get_cutoff_date() }} BETWEEN CA.ValidFrom AND CA.ValidTo
        ) CU ON
            CU.CustomerID = I.CustomerID LEFT JOIN
        (
            SELECT
                C.CityID,
                C.CityName
            FROM
                {{ source('WideWorldImporters', 'Application_Cities') }} AS C
            WHERE
                {{ get_cutoff_date() }} BETWEEN C.ValidFrom AND C.ValidTo

            UNION ALL

            SELECT
                CA.CityID,
                CA.CityName
            FROM
                {{ source('WideWorldImporters', 'Application_Cities_Archive') }} AS CA
            WHERE
                {{ get_cutoff_date() }} BETWEEN CA.ValidFrom AND CA.ValidTo
        ) C ON
            C.CityID = CU.DeliveryCityID LEFT JOIN
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
        ) BCU ON
            BCU.CustomerID = I.BillToCustomerID LEFT JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName,
                SI.IsChillerStock
            FROM
                {{ source('WideWorldImporters', 'Warehouse_StockItems') }} AS SI
            WHERE
                {{ get_cutoff_date() }} BETWEEN SI.ValidFrom AND SI.ValidTo

            UNION ALL

            SELECT
                SIA.StockItemID,
                SIA.StockItemName,
                SIA.IsChillerStock
            FROM
                {{ source('WideWorldImporters', 'Warehouse_StockItems_Archive') }} AS SIA
            WHERE
                {{ get_cutoff_date() }} BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = IL.StockItemID LEFT JOIN
        (
            SELECT
                P.PersonID,
                P.FullName
            FROM
                {{ source('WideWorldImporters', 'Application_People') }} AS P
            WHERE
                {{ get_cutoff_date() }} BETWEEN P.ValidFrom AND P.ValidTo

            UNION ALL

            SELECT
                PA.PersonID,
                PA.FullName
            FROM
                {{ source('WideWorldImporters', 'Application_People_Archive') }} AS PA
            WHERE
                {{ get_cutoff_date() }} BETWEEN PA.ValidFrom AND PA.ValidTo
        ) SP ON
            SP.PersonID = I.SalespersonPersonID LEFT JOIN
        (
            SELECT
                PT.PackageTypeID,
                PT.PackageTypeName
            FROM
                {{ source('WideWorldImporters', 'Warehouse_PackageTypes') }} AS PT
            WHERE
                {{ get_cutoff_date() }} BETWEEN PT.ValidFrom AND PT.ValidTo

            UNION ALL

            SELECT
                PTA.PackageTypeID,
                PTA.PackageTypeName
            FROM
                {{ source('WideWorldImporters', 'Warehouse_PackageTypes_Archive') }} AS PTA
            WHERE
                {{ get_cutoff_date() }} BETWEEN PTA.ValidFrom AND PTA.ValidTo
        ) PT ON
            PT.PackageTypeID = IL.PackageTypeID
    WHERE
        I.LastEditedWhen <= {{ get_cutoff_date() }} AND
        IL.LastEditedWhen <= {{ get_cutoff_date() }}
)

SELECT
    *
FROM
    sales