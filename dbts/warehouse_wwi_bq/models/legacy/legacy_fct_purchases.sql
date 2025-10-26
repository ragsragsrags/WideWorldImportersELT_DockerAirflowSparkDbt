WITH purchases AS (

    SELECT
        PO.OrderDate AS DateKey,
        PO.PurchaseOrderID AS WWIPurchaseOrderID,
        POL.PurchaseOrderLineID AS WWIPurchaseOrderLineID,
        S.SupplierID AS WWISupplierID,
        SI.StockItemID AS WWIStockItemID,
        POL.OrderedOuters,
        POL.OrderedOuters * SI.QuantityPerOuter AS OrderedQuantity,
        POL.ReceivedOuters,
        PT.PackageTypeName AS Package,
        PO.IsOrderFinalized AS IsOrderFinalized
    FROM
        {{ source('WideWorldImporters', 'Purchasing_PurchaseOrders') }} AS PO LEFT JOIN
        {{ source('WideWorldImporters', 'Purchasing_PurchaseOrderLines') }} AS POL ON
            POL.PurchaseOrderID = PO.PurchaseOrderID LEFT JOIN
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
            S.SupplierID = PO.SupplierID LEFT JOIN
        (
            SELECT
                SI.StockItemID,
                SI.StockItemName,
                SI.QuantityPerOuter
            FROM
                {{ source('WideWorldImporters', 'Warehouse_StockItems') }} AS SI 
            WHERE
                {{ get_cutoff_date() }} BETWEEN SI.ValidFrom AND SI.ValidTo

            UNION ALL

            SELECT
                SIA.StockItemID,
                SIA.StockItemName,
                SIA.QuantityPerOuter
            FROM
                {{ source('WideWorldImporters', 'Warehouse_StockItems_Archive') }} AS SIA
            WHERE
                {{ get_cutoff_date() }} BETWEEN SIA.ValidFrom AND SIA.ValidTo
        ) SI ON
            SI.StockItemID = POL.StockItemID LEFT JOIN
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
            PT.PackageTypeID = POL.PackageTypeID
    WHERE
        PO.LastEditedWhen <= {{ get_cutoff_date() }} AND
        POL.LastEditedWhen <= {{ get_cutoff_date() }}

)

SELECT
    *
FROM
    purchases