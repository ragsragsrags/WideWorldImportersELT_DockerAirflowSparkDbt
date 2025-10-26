WITH stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

changed_stock_holdings AS (

    SELECT DISTINCT
		stock_item_holdings.* 
    FROM 
		{{ ref('stg_warehouse_stock_item_holdings') }} AS stock_item_holdings 
    WHERE
        StockItemID IN (
            SELECT
                WWIStockItemID
            FROM 
                stock_items
        )    

)

SELECT
    *
FROM
    changed_stock_holdings