WITH stock_holdings AS (

    SELECT 
        SI.StockItemID AS WWIStockItemID,
        SIH.QuantityOnHand,
        SIH.BinLocation,
        SIH.LastStocktakeQuantity,
        SIH.LastCostPrice,
        SIH.ReorderLevel,
        SIH.TargetStockLevel
    FROM
        {{ source('WideWorldImporters', 'Warehouse_StockItemHoldings') }} AS SIH JOIN
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
            SI.StockItemID = SIH.StockItemID

)

SELECT
    *
FROM
    stock_holdings