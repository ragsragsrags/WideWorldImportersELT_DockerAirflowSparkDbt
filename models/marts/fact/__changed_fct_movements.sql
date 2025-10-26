WITH changed_movements AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_fct_movements', 
            'stg_warehouse_stock_item_transactions', 
            '', 
            'LastEditedWhen',
            '',
            'StockItemTransactionID' 
        ) 
    }}

)

SELECT
    *
FROM
    changed_movements