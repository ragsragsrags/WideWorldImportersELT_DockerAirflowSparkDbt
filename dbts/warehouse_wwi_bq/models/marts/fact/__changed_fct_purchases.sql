WITH cutoff_dates AS 
(

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_fct_purchases') }} AS last_cutoff_date

),

changed_packaged_types AS 
(

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_purchases', 
            'stg_warehouse_package_types', 
            'stg_warehouse_package_types_archive', 
            'ValidFrom',
            'ValidTo',
            'PackageTypeID' 
        ) 
    }}

),

changed_stock_items AS 
(

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_purchases', 
            'stg_warehouse_stock_items', 
            'stg_warehouse_stock_items_archive', 
            'ValidFrom',
            'ValidTo',
            'StockItemID' 
        ) 
    }}

),

changed_purchases AS (

    SELECT DISTINCT
		purchase_orders.* 
    FROM 
		{{ ref('stg_purchasing_purchase_orders') }} AS purchase_orders JOIN 
		{{ ref('stg_purchasing_purchase_order_lines') }} AS purchase_order_lines ON 
			purchase_orders.PurchaseOrderID = purchase_order_lines.PurchaseOrderID
    WHERE 
		(
			purchase_orders.LastEditedWhen > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
			purchase_order_lines.LastEditedWhen > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
			purchase_order_lines.PackageTypeID IN (
                SELECT 
                    PackageTypeID
                FROM 
                    changed_packaged_types
            ) OR
			purchase_order_lines.StockItemID IN (
                SELECT 
                    StockItemID
                FROM 
                    changed_stock_items
            )
		) AND
		purchase_orders.LastEditedWhen <= (SELECT cutoff_date FROM cutoff_dates LIMIT 1) AND
		purchase_order_lines.LastEditedWhen <= (SELECT cutoff_date FROM cutoff_dates LIMIT 1)

)

SELECT
    *
FROM
    changed_purchases