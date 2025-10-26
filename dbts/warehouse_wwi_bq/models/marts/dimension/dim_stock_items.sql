WITH stock_items AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_dim_stock_items') }}      

),

package_types AS (

    {{ 
        get_table_merged
        (
            'stg_warehouse_package_types', 
            'stg_warehouse_package_types_archive'
        ) 
    }}

),

colors AS (

    {{ 
        get_table_merged
        (
            'stg_warehouse_colors', 
            'stg_warehouse_colors_archive'
        ) 
    }}

),

final AS (
        
    SELECT
        ROW_NUMBER() OVER (ORDER BY stock_items.StockItemID) AS StockItemKey,
        stock_items.StockItemID AS WWIStockItemID,
        stock_items.StockItemName AS StockItem,
        CASE 
            WHEN colors.ColorName IS NOT NULL THEN colors.ColorName
            ELSE 'N/A'
        END AS Color,
        package_types.PackageTypeName AS SellingPackage,
        buyer_package_types.PackageTypeName AS BuyingPackage,
        stock_items.Brand,
        stock_items.Size,
        stock_items.LeadTimeDays,
        stock_items.QuantityPerOuter,
        stock_items.IsChillerStock,
        stock_items.Barcode,
        stock_items.TaxRate,
        stock_items.UnitPrice,
        stock_items.RecommendedRetailPrice,
        stock_items.TypicalWeightPerUnit,
        stock_items.Photo
    FROM
        stock_items LEFT JOIN
        package_types ON
            stock_items.UnitPackageID = package_types.PackageTypeID LEFT JOIN
        package_types AS buyer_package_types ON
            stock_items.OuterPackageID = buyer_package_types.PackageTypeID LEFT JOIN
        colors ON
            stock_items.ColorID = colors.ColorID 

    UNION ALL

    SELECT
        0 AS StockItemKey,
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

)

SELECT 
    * 
FROM 
    final
ORDER BY
    StockItemKey