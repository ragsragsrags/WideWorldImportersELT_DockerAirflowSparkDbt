WITH movements AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_fct_movements') }}      

),

stock_items AS (

    SELECT
        *
    FROM
        {{ ref('dim_stock_items') }}

),

customers AS (

    SELECT
        *
    FROM
        {{ ref('dim_customers') }}

),

dates AS (

    SELECT
        *
    FROM
        {{ ref('dim_dates') }}

),

suppliers AS (

    SELECT
        *
    FROM
        {{ ref('dim_suppliers') }}

),

transaction_types AS (

    SELECT
        *
    FROM
        {{ ref('dim_transaction_types') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY movements.StockItemTransactionID) AS MovementKey,
        dates.date AS DateKey,
        IFNULL(stock_items.StockItemKey, 0) AS StockItemKey,
        IFNULL(customers.CustomerKey, 0) AS CustomerKey,
        IFNULL(suppliers.SupplierKey, 0) AS SupplierKey,
        IFNULL(transaction_types.TransactionTypeKey, 0) AS TransactionTypeKey,
        movements.StockItemTransactionID AS WWIStockItemTransactionID,
        movements.InvoiceID AS WWIInvoiceID,
        movements.PurchaseOrderID AS WWIPurchaseOrderID,
        movements.Quantity,
        stock_items.WWIStockItemID,
        customers.WWICustomerID,
        suppliers.WWISupplierID,
        transaction_types.WWITransactionTypeID
    FROM
        movements LEFT JOIN
        dates ON
            movements.TransactionOccurredWhen = dates.date LEFT JOIN
        stock_items ON
            movements.StockItemID = stock_items.WWIStockItemID LEFT JOIN
        customers ON
            movements.CustomerID = customers.WWICustomerID LEFT JOIN
        suppliers ON
            movements.SupplierID = suppliers.WWISupplierID LEFT JOIN
        transaction_types ON
            movements.TransactionTypeID = transaction_types.WWITransactionTypeID
)

SELECT 
    * 
FROM 
    final