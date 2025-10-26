WITH orders AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_fct_orders') }}      

),

order_lines AS (

    SELECT
        *
    FROM
        {{ ref("stg_sales_order_lines") }}
    WHERE
        OrderID IN (
            SELECT
                OrderID
            FROM
                orders
        )

),

package_types AS (

    SELECT
        *
    FROM
        {{ ref('stg_warehouse_package_types') }}

),

cities AS (

    SELECT
        *
    FROM
        {{ ref('dim_cities') }}

),

customers AS (

    SELECT
        *
    FROM
        {{ ref('dim_customers') }}

),

stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

employees AS (

    SELECT
        *
    FROM
        {{ ref('dim_employees') }}

),

dates AS (

    SELECT
        *
    FROM
        {{ ref('dim_dates') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY orders.OrderID) AS OrderKey,
        IFNULL(cities.CityKey, 0) AS CityKey,
        IFNULL(customers.CustomerKey, 0) AS CustomerKey,
        IFNULL(stock_items.StockItemKey, 0) AS StockItemKey,
        dates.date AS OrderDateKey,
        pick_dates.date AS PickedDateKey,
        IFNULL(employees.EmployeeKey, 0) AS SalesPersonKey,
        IFNULL(picker.EmployeeKey, 0) AS PickerKey,
        orders.OrderID AS WWIOrderID,
        order_lines.OrderLineID AS WWIOrderLineID,
        orders.BackorderOrderID AS WWIBackorderID,
        order_lines.Description,
        package_types.PackageTypeName AS Package,
        order_lines.Quantity,
        order_lines.UnitPrice,
        order_lines.TaxRate,
        order_lines.TotalExcludingTax,
        order_lines.TaxAmount,
        order_lines.TotalIncludingTax,
        cities.WWICityID,
        customers.WWICustomerID,
        stock_items.WWIStockItemID,
        employees.WWIEmployeeID AS WWISalesPersonID,
        picker.WWIEmployeeID AS WWIPickerID
    FROM
        orders JOIN
        order_lines ON
            orders.OrderID = order_lines.OrderID LEFT JOIN
        customers ON
            orders.CustomerID = customers.WWICustomerID LEFT JOIN
        cities ON
            customers.WWIDeliveryCityID = cities.WWICityID LEFT JOIN
        stock_items ON
            order_lines.StockItemID = stock_items.WWIStockItemID LEFT JOIN
        dates ON
            orders.OrderDate = dates.Date LEFT JOIN
        dates AS pick_dates ON
            orders.PickingCompletedWhen = pick_dates.Date LEFT JOIN
        employees ON
            orders.SalesPersonPersonID = employees.WWIEmployeeID LEFT JOIN
        employees AS picker ON
            orders.PickedByPersonID = picker.WWIEmployeeID LEFT JOIN
        package_types ON
            order_lines.PackageTypeID = package_types.PackageTypeID 
                
)

SELECT
    *
FROM
    final