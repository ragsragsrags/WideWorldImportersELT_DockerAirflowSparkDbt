WITH orders AS 
(

    SELECT
        O.OrderID AS WWIOrderID,
        OL.OrderLineID AS WWIOrderLineID,
        CAST(O.OrderDate AS DATE) AS OrderDateKey,
        CAST(O.PickingCompletedWhen AS DATE) AS PickedDateKey,
        OL.Description,
        PT.PackageTypeName AS Package,
        OL.Quantity,
        OL.UnitPrice,
        OL.TaxRate,
        ROUND(OL.Quantity * OL.UnitPrice, 2) AS TotalExcludingTax,
        ROUND(OL.Quantity * OL.UnitPrice * OL.TaxRate / 100.0, 2) AS TaxAmount,
        ROUND(OL.Quantity * OL.UnitPrice, 2) + ROUND(OL.Quantity * OL.UnitPrice * OL.TaxRate / 100.0, 2) AS TotalIncludingTax,
        C.DeliveryCityID AS WWICityID,
        C.CustomerID AS WWICustomerID,
        SI.StockItemID AS WWIStockItemID,
        O.SalespersonPersonID AS WWISalesPersonID,
        O.PickedByPersonID AS WWIPickerID,
        O.BackorderOrderID AS WWIBackorderID
    FROM
        {{ source('WideWorldImporters', 'Sales_Orders') }} AS O LEFT JOIN
        {{ source('WideWorldImporters', 'Sales_OrderLines') }} AS OL ON
            OL.OrderID = O.OrderID LEFT JOIN 
        (
            SELECT
                C.CustomerID,
                C.DeliveryCityID,
                C.CustomerName
            FROM
                {{ source('WideWorldImporters', 'Sales_Customers') }} AS C
            WHERE
                {{ get_cutoff_date() }} BETWEEN C.ValidFrom AND C.ValidTo

            UNION ALL

            SELECT
                CA.CustomerID,
                CA.DeliveryCityID,
                CA.CustomerName
            FROM
                {{ source('WideWorldImporters', 'Sales_Customers_Archive') }} AS CA
            WHERE
                {{ get_cutoff_date() }} BETWEEN CA.ValidFrom AND CA.ValidTo
        ) C ON
            C.CustomerID = O.CustomerID LEFT JOIN
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
        ) CI ON
            CI.CityID = C.DeliveryCityID LEFT JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName
            FROM
                {{ source('WideWorldImporters', 'Warehouse_StockItems') }} AS SI 
            WHERE
                {{ get_cutoff_date() }} BETWEEN SI.ValidFrom AND SI.ValidTo

            UNION ALL

            SELECT
                SIA.StockItemID,
                SIA.StockItemName
            FROM
                {{ source('WideWorldImporters', 'Warehouse_StockItems_Archive') }} AS SIA 
            WHERE
                {{ get_cutoff_date() }} BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = OL.StockItemID LEFT JOIN
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
        ) P ON
            P.PersonID = O.SalespersonPersonID LEFT JOIN
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
        ) P2 ON
            P2.PersonID = O.PickedByPersonID LEFT JOIN
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
            PT.PackageTypeID = OL.PackageTypeID
    WHERE
        O.LastEditedWhen <= {{ get_cutoff_date() }} AND
	    OL.LastEditedWhen <= {{ get_cutoff_date() }}

)

SELECT
    *
FROM
    orders
