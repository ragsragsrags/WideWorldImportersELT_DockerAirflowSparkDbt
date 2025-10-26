WITH transactions AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_fct_transactions') }}      

),

customers AS (
    
    SELECT
        *
    FROM
        {{ ref('dim_customers') }}

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

payment_methods AS (

    SELECT
        *
    FROM
        {{ ref('dim_payment_methods') }}

),

dates AS (

    SELECT
        *
    FROM
        {{ ref('dim_dates') }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (
            ORDER BY 
                transactions.TransactionDate,
                transactions.CustomerTransactionID,
                transactions.SupplierTransactionID
        ) AS TransactionKey,
        transactions.TransactionDate AS DateKey,
        customers.CustomerKey,
        bill_to_customers.CustomerKey AS BillToCustomerKey,
        suppliers.SupplierKey,
        IFNULL(transaction_types.TransactionTypeKey, 0) AS TransactionTypeKey,
        payment_methods.PaymentMethodKey,
        transactions.CustomerTransactionID AS WWICustomerTransactionID,
        transactions.SupplierTransactionID AS WWISupplierTransactionID,
        customers.WWICustomerID,
        bill_to_customers.WWICustomerID AS WWIBillToCustomerID,
        suppliers.WWISupplierID,
        transaction_types.WWITransactionTypeID,
        payment_methods.WWIPaymentMethodID,
        transactions.InvoiceID AS WWIInvoiceID,
        transactions.PurchaseOrderID AS WWIPurchaseOrderID,
        transactions.SupplierInvoiceNumber,
        transactions.TotalExcludingTax,
        transactions.TaxAmount,
        transactions.TotalIncludingTax,
        transactions.OutstandingBalance,
        transactions.IsFinalized
    FROM
        transactions LEFT JOIN 
        dates ON
            transactions.TransactionDate = dates.Date LEFT JOIN
        customers ON
            transactions.CustomerID = customers.WWICustomerID LEFT JOIN
        customers AS bill_to_customers ON
            transactions.BillToCustomerID = bill_to_customers.WWICustomerID LEFT JOIN
        suppliers ON
            transactions.SupplierID = suppliers.WWISupplierID LEFT JOIN
        transaction_types ON
            transactions.TransactionTypeID = transaction_types.WWITransactionTypeID LEFT JOIN
        payment_methods ON
            transactions.PaymentMethodID = payment_methods.WWIPaymentMethodID    

)

SELECT
    *
FROM
    final