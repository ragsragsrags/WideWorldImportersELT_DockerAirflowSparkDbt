WITH customer_transactions AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_transactions', 
            'stg_sales_customer_transactions', 
            '', 
            'LastEditedWhen',
            '',
            'CustomerTransactionID' 
        ) 
    }}

),

supplier_transactions AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_transactions', 
            'stg_purchasing_supplier_transactions', 
            '', 
            'LastEditedWhen',
            '',
            'SupplierTransactionID' 
        ) 
    }}

),

invoices AS (

    SELECT
        *
    FROM
        {{ ref('stg_sales_invoices') }}

),

final AS (

    SELECT
        customer_transactions.CustomerTransactionID,
        CAST(NULL AS INTEGER) AS SupplierTransactionID,
        COALESCE(invoices.CustomerID, customer_transactions.CustomerID) AS CustomerID,
        customer_transactions.CustomerID AS BillToCustomerID,
        CAST(NULL AS INTEGER) AS SupplierID,
        customer_transactions.TransactionTypeID,
        customer_transactions.InvoiceID,
        CAST(NULL AS INTEGER) AS PurchaseOrderID,
        customer_transactions.PaymentMethodID,
        CAST(NULL AS STRING) AS SupplierInvoiceNumber,
        customer_transactions.TransactionDate,
        customer_transactions.TotalExcludingTax,
        customer_transactions.TaxAmount,
        customer_transactions.TotalIncludingTax,
        customer_transactions.OutstandingBalance,
        customer_transactions.FinalizationDate,
        customer_transactions.IsFinalized,
        customer_transactions.LastEditedBy,
        customer_transactions.LastEditedWhen
    FROM
        customer_transactions LEFT JOIN
        invoices ON
            invoices.InvoiceID = customer_transactions.InvoiceID

    UNION ALL

    SELECT
        CAST(NULL AS INTEGER) AS CustomerTransactionID,
        supplier_transactions.SupplierTransactionID,
        CAST(NULL AS INTEGER) AS CustomerID,
        CAST(NULL AS INTEGER) AS BillToCustomerID,
        supplier_transactions.SupplierID,        
        supplier_transactions.TransactionTypeID,
        CAST(NULL AS INTEGER) AS InvoiceID,
        supplier_transactions.PurchaseOrderID,
        supplier_transactions.PaymentMethodID,
        supplier_transactions.SupplierInvoiceNumber,
        supplier_transactions.TransactionDate,
        supplier_transactions.TotalExcludingTax,
        supplier_transactions.TaxAmount,
        supplier_transactions.TotalIncludingTax,
        supplier_transactions.OutstandingBalance,
        supplier_transactions.FinalizationDate,
        supplier_transactions.IsFinalized,
        supplier_transactions.LastEditedBy,
        supplier_transactions.LastEditedWhen
    FROM
        supplier_transactions

)

SELECT
    *
FROM
    final