WITH cutoff_dates AS 
(

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_fct_sales') }} AS last_cutoff_date

),

stock_items AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_sales', 
            'stg_warehouse_stock_items', 
            'stg_warehouse_stock_items_archive', 
            'ValidFrom',
            'ValidTo',
            'StockItemID' 
        ) 
    }}

),

package_types AS 
(

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_sales', 
            'stg_warehouse_package_types', 
            'stg_warehouse_package_types_archive', 
            'ValidFrom',
            'ValidTo',
            'PackageTypeID' 
        ) 
    }}

),

customers AS 
(

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_sales', 
            'stg_sales_customers', 
            'stg_sales_customers_archive', 
            'ValidFrom',
            'ValidTo',
            'CustomerID' 
        ) 
    }}

),

changed_sales AS 
(

    SELECT DISTINCT
		invoices.* 
    FROM 
		{{ ref('stg_sales_invoices') }} AS invoices JOIN 
		{{ ref('stg_sales_invoice_lines') }} AS invoice_lines ON 
			invoices.InvoiceID = invoice_lines.InvoiceID 
    WHERE 
		(
			invoices.LastEditedWhen > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
			invoice_lines.LastEditedWhen > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
			invoice_lines.PackageTypeID IN (
                SELECT 
                    PackageTypeID
                FROM 
                    package_types
            ) OR
			invoice_lines.StockItemID IN (
                SELECT 
                    StockItemID
                FROM 
                    stock_items
            ) OR
			invoices.CustomerID IN (
                SELECT 
                    CustomerID
                FROM 
                    customers
            ) OR
			invoices.BillToCustomerID IN (
                SELECT 
                    CustomerID
                FROM 
                    customers
            )
		) AND
		invoices.LastEditedWhen <= (SELECT cutoff_date FROM cutoff_dates LIMIT 1) AND
		invoice_lines.LastEditedWhen <= (SELECT cutoff_date FROM cutoff_dates LIMIT 1)

)

SELECT
    *
FROM
    changed_sales