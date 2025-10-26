WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Warehouse_StockItems') }}
    
),

final AS (

    SELECT
        StockItemID,
        StockItemName,
        ColorID,
        UnitPackageID,
        OuterPackageID,
        Brand,
        Size,
        LeadTimeDays,
        QuantityPerOuter,
        IsChillerStock,
        Barcode,
        TaxRate,
        UnitPrice,
        RecommendedRetailPrice,
        TypicalWeightPerUnit,
        Photo,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    * 
FROM 
    final