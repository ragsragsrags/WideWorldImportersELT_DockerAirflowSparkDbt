WITH purchase_orders AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_fct_purchases') }}      

),

purchase_order_lines AS (

    SELECT
        *
    FROM
        {{ ref("stg_purchasing_purchase_order_lines") }}
    WHERE
        PurchaseOrderID IN (
            SELECT
                PurchaseOrderID
            FROM
                purchase_orders
        )

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

stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

suppliers AS (

    SELECT
        *
    FROM
        {{ ref('dim_suppliers') }}

),

dates AS (

    SELECT
        *
    FROM
        {{ ref('dim_dates') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY purchase_orders.PurchaseOrderID) AS PurchaseKey,
        dates.Date AS DateKey,
        suppliers.SupplierKey,
        stock_items.StockItemKey,
        purchase_orders.PurchaseOrderID AS WWIPurchaseOrderID,
        purchase_order_lines.PurchaseOrderLineID AS WWIPurchaseOrderLineID,
        suppliers.WWISupplierID,
        stock_items.WWIStockItemID,
        purchase_order_lines.OrderedOuters,
        (
            purchase_order_lines.OrderedOuters * 
            stock_items.QuantityPerOuter
        ) AS OrderedQuantity,
        purchase_order_lines.ReceivedOuters,
        package_types.PackageTypeName AS Package,
        purchase_order_lines.IsOrderLineFinalized AS IsOrderFinalized
    FROM
        purchase_orders JOIN
        purchase_order_lines ON
            purchase_orders.PurchaseOrderID = purchase_order_lines.PurchaseOrderID LEFT JOIN
        dates ON
            purchase_orders.OrderDate = dates.Date LEFT JOIN
        package_types ON
            purchase_order_lines.PackageTypeID = package_types.PackageTypeID LEFT JOIN
        stock_items ON
            purchase_order_lines.StockItemID = stock_items.WWIStockItemID LEFT JOIN
        suppliers ON
            purchase_orders.SupplierID = suppliers.WWISupplierID 
                
)

SELECT
    *
FROM
    final