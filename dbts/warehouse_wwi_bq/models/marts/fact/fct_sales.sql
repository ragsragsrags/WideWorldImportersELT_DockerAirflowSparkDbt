WITH invoices AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_fct_sales') }}      

),

invoice_lines AS (

    SELECT
        *
    FROM
        {{ ref("stg_sales_invoice_lines") }}
    WHERE
        InvoiceID IN (
            SELECT
                InvoiceID
            FROM
                invoices
        )

),

stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

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

customers AS (

    SELECT
        *
    FROM
        {{ ref('dim_customers') }}

),

cities AS (

    SELECT
        *
    FROM 
        {{ ref('dim_cities') }}

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
        ROW_NUMBER() OVER (ORDER BY invoices.InvoiceID) AS SaleKey,
        IFNULL(cities.CityKey, 0) AS CityKey,
        IFNULL(customers.CustomerKey, 0) AS CustomerKey,
        IFNULL(bill_customers.CustomerKey, 0) AS BillToCustomerKey,
        IFNULL(stock_items.StockItemKey, 0) AS StockItemKey,
        dates.Date AS InvoiceDateKey,
        delivery_dates.Date AS DeliveryDateKey,
        IFNULL(employees.EmployeeKey, 0) AS SalesPersonKey,
        invoices.InvoiceId AS WWIInvoiceID,
        invoice_lines.InvoiceLineID AS WWIInvoiceLineID,
        cities.WWICityID,
        customers.WWICustomerID,
        bill_customers.WWICustomerID AS WWIBillToCustomerID,
        stock_items.WWIStockItemID,
        employees.WWIEmployeeID AS WWISalesPersonID,
        invoice_lines.Description,
        package_types.PackageTypeName AS Package,
        invoice_lines.Quantity,
        invoice_lines.UnitPrice,
        invoice_lines.TaxRate,
        invoice_lines.TotalExcludingTax,
        invoice_lines.TaxAmount,
        invoice_lines.LineProfit AS Profit,
        invoice_lines.TotalIncludingTax,
        CASE 
            WHEN stock_items.IsChillerStock = FALSE THEN invoice_lines.Quantity 
            ELSE 0
        END AS TotalDryItems,
        CASE
            WHEN stock_items.IsChillerStock = TRUE THEN invoice_lines.Quantity 
            ELSE 0
        END AS TotalChillerItems
    FROM
        invoices JOIN
        invoice_lines ON
            invoices.InvoiceID = invoice_lines.InvoiceID LEFT JOIN
        stock_items ON
            invoice_lines.StockItemID = stock_items.WWIStockItemID LEFT JOIN
        dates ON
            invoices.InvoiceDate = dates.Date LEFT JOIN
        dates AS delivery_dates ON
            invoices.ConfirmedDeliveryTime = delivery_dates.Date LEFT JOIN
        package_types ON
            invoice_lines.PackageTypeID = package_types.PackageTypeID LEFT JOIN
        customers ON
            invoices.CustomerID = customers.WWICustomerID LEFT JOIN
        cities ON
            customers.WWIDeliveryCityID = cities.WWICityID LEFT JOIN
        customers AS bill_customers ON
            invoices.BillToCustomerID = bill_customers.WWICustomerID LEFT JOIN
        employees ON
             invoices.SalesPersonPersonID = employees.WWIEmployeeID
                
)

SELECT
    *
FROM
    final