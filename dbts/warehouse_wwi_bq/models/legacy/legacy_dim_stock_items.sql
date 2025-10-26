SELECT
    SI.StockItemID AS WWIStockItemID,
    SI.StockItemName AS StockItem,
    CASE 
        WHEN ColorName IS NOT NULL THEN ColorName
        ELSE 'N/A'
    END AS Color,
    SP.PackageTypeName AS SellingPackage,
    BP.PackageTypeName AS BuyingPackage,
    SI.Brand,
    SI.Size,
    SI.LeadTimeDays,
    SI.QuantityPerOuter,
    SI.IsChillerStock,
    SI.Barcode,
    SI.TaxRate,
    SI.UnitPrice,
    SI.RecommendedRetailPrice,
    SI.TypicalWeightPerUnit,
    SI.Photo
FROM
    (
        SELECT
            SI.StockItemID,
            SI.StockItemName,
            SI.ColorID,
            SI.OuterPackageID,
            SI.UnitPackageID,
            SI.Brand,
            SI.Size,
            SI.LeadTimeDays,
            SI.QuantityPerOuter,
            SI.IsChillerStock,
            SI.Barcode,
            SI.TaxRate,
            SI.UnitPrice,
            SI.RecommendedRetailPrice,
            SI.TypicalWeightPerUnit,
            SI.Photo
        FROM
            {{ source('WideWorldImporters', 'Warehouse_StockItems') }} AS SI
        WHERE
            {{ get_cutoff_date() }} BETWEEN SI.ValidFrom AND SI.ValidTo

        UNION ALL

        SELECT
            SIA.StockItemID,
            SIA.StockItemName,
            SIA.ColorID,
            SIA.OuterPackageID,
            SIA.UnitPackageID,
            SIA.Brand,
            SIA.Size,
            SIA.LeadTimeDays,
            SIA.QuantityPerOuter,
            SIA.IsChillerStock,
            SIA.Barcode,
            SIA.TaxRate,
            SIA.UnitPrice,
            SIA.RecommendedRetailPrice,
            SIA.TypicalWeightPerUnit,
            SIA.Photo
        FROM
            {{ source('WideWorldImporters', 'Warehouse_StockItems_Archive') }} AS SIA
        WHERE
            {{ get_cutoff_date() }} BETWEEN SIA.ValidFrom AND SIA.ValidTo
    ) SI LEFT JOIN
    (
        SELECT
            C.ColorID,
            C.ColorName
        FROM
            {{ source('WideWorldImporters', 'Warehouse_Colors') }} AS C 
        WHERE
            {{ get_cutoff_date() }} BETWEEN C.ValidFrom AND C.ValidTo

        UNION ALL

        SELECT
            CA.ColorID,
            CA.ColorName
        FROM
            {{ source('WideWorldImporters', 'Warehouse_Colors_Archive') }} AS CA 
        WHERE
            {{ get_cutoff_date() }} BETWEEN CA.ValidFrom AND CA.ValidTo
    ) C ON
        C.ColorID = SI.ColorID LEFT JOIN
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
    ) SP ON
        SP.PackageTypeID = SI.UnitPackageID LEFT JOIN
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
    ) BP ON
        BP.PackageTypeID = SI.OuterPackageID

UNION ALL

SELECT
    0 AS WWIStockItemID,
    'Unknown' AS StockItem,
    'N/A' AS Color,
    'N/A' AS SellingPackage,
    'N/A' AS BuyingPackage,
    'N/A' AS Brand,
    'N/A' AS Size,
    0 AS LeadTimeDays,
    0 AS QuantityPerOuter,	
    TRUE AS IsChillerStock,
    'N/A' AS Barcode,
    0.000 AS TaxRate,
    0.00 AS UnitPrice,
    0.00 AS RecommendedRetailPrice,
    0.000 TypicalWeightPerUnit,
    CAST(NULL AS BYTES) Photo