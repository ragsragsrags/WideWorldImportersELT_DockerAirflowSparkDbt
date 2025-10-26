WITH stock_holdings AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_fct_stock_holdings') }}      

),

stock_items AS (
    
    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY stock_holdings.StockItemID) AS StockHoldingKey,
        stock_items.StockItemKey,
        stock_items.WWIStockItemID,
        stock_holdings.QuantityOnHand,
        stock_holdings.BinLocation,
        stock_holdings.LastStocktakeQuantity,
        stock_holdings.LastCostPrice,
        stock_holdings.ReorderLevel,
        stock_holdings.TargetStockLevel
    FROM
        stock_holdings JOIN
        stock_items ON
            stock_holdings.StockItemID = stock_items.WWIStockItemID 
                
)

SELECT
    *
FROM
    final