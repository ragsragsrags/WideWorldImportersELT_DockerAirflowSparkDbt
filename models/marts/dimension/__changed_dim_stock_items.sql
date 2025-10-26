WITH cutoff_dates AS (

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_dim_stock_items') }} AS last_cutoff_date

),

changed_package_types AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_stock_items', 
            'stg_warehouse_package_types', 
            'stg_warehouse_package_types_archive', 
            'ValidFrom',
            'ValidTo',
            'PackageTypeID' 
        )

    }}

),

changed_colors AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_stock_items', 
            'stg_warehouse_colors', 
            'stg_warehouse_colors_archive', 
            'ValidFrom',
            'ValidTo',
            'ColorID' 
        )

    }}

),

stock_items AS (

    SELECT
        stock_items.*
    FROM
        {{ ref('stg_warehouse_stock_items') }} AS stock_items
    WHERE
        (
            stock_items.ValidFrom > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
            stock_items.UnitPackageID IN
            (
                SELECT
                    PackageTypeID
                FROM
                    changed_package_types
            ) OR
            stock_items.OuterPackageID IN
            (
                SELECT
                    PackageTypeID
                FROM
                    changed_package_types
            ) OR
            stock_items.ColorID IN
            (
                SELECT
                    ColorID
                FROM
                    changed_colors
            ) 
        ) AND
        (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN stock_items.ValidFrom AND stock_items.ValidTo

),

stock_items_archive AS (

    SELECT
        stock_items_archive.*
    FROM
        {{ ref('stg_warehouse_stock_items_archive') }} AS stock_items_archive
    WHERE
        (
            stock_items_archive.ValidFrom > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
            stock_items_archive.UnitPackageID IN
            (
                SELECT
                    PackageTypeID
                FROM
                    changed_package_types
            ) OR
            stock_items_archive.OuterPackageID IN
            (
                SELECT
                    PackageTypeID
                FROM
                    changed_package_types
            ) OR
            stock_items_archive.ColorID IN
            (
                SELECT
                    ColorID
                FROM
                    changed_colors
            ) 
        ) AND
        (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN stock_items_archive.ValidFrom AND stock_items_archive.ValidTo 

),

final AS (

    SELECT
        stock_items.*
    FROM
        stock_items
    
    UNION ALL

    SELECT
        stock_items_archive.*
    FROM
        stock_items_archive 

)

SELECT 
    *
FROM
    final