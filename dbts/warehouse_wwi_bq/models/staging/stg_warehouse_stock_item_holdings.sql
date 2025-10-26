WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Warehouse_StockItemHoldings') }}

),

transformed AS (

    SELECT
        StockItemID,
        QuantityOnHand,
        BinLocation,
        LastStocktakeQuantity,
        LastCostPrice,
        ReorderLevel,
        TargetStockLevel,
        LastEditedBy,
        LastEditedWhen
    FROM
        source

)

SELECT
    *
FROM
    transformed