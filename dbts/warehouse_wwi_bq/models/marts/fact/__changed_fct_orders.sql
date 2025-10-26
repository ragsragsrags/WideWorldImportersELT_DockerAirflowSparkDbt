WITH cutoff_dates AS (

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_fct_orders') }} AS last_cutoff_date

),

changed_packaged_types AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_orders', 
            'stg_warehouse_package_types', 
            'stg_warehouse_package_types_archive', 
            'ValidFrom',
            'ValidTo',
            'PackageTypeID' 
        ) 
    }}

),

changed_orders AS (

    SELECT DISTINCT
		orders.* 
    FROM 
		{{ ref('stg_sales_orders') }} AS orders JOIN 
		{{ ref('stg_sales_order_lines') }} AS order_lines ON 
			orders.OrderID = order_lines.OrderID 
    WHERE 
		(
			orders.LastEditedWhen > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
			order_lines.LastEditedWhen > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
			order_lines.PackageTypeID IN (
                SELECT 
                    PackageTypeID
                FROM 
                    changed_packaged_types
            )
		) AND
		orders.LastEditedWhen <= (SELECT cutoff_date FROM cutoff_dates LIMIT 1) AND
		order_lines.LastEditedWhen <= (SELECT cutoff_date FROM cutoff_dates LIMIT 1)

)

SELECT
    *
FROM
    changed_orders