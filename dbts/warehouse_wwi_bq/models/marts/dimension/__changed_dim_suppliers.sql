WITH cutoff_dates AS (

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_dim_suppliers') }} AS last_cutoff_date

),

changed_supplier_categories AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_suppliers', 
            'stg_purchasing_supplier_categories', 
            'stg_purchasing_supplier_categories_archive', 
            'ValidFrom',
            'ValidTo',
            'SupplierCategoryID' 
        ) 
    }}

),

changed_people AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_suppliers', 
            'stg_application_people', 
            'stg_application_people_archive', 
            'ValidFrom',
            'ValidTo',
            'PersonID' 
        ) 
    }}

),

suppliers AS (

    SELECT
        suppliers.*
    FROM
        {{ ref('stg_purchasing_suppliers') }} AS suppliers
    WHERE
        (
            suppliers.ValidFrom > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
            suppliers.SupplierCategoryID IN
            (
                SELECT
                    SupplierCategoryID
                FROM
                    changed_supplier_categories
            ) OR
            suppliers.PrimaryContactPersonID IN
            (
                SELECT
                    PersonID
                FROM
                    changed_people
            )
        ) AND
        (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN suppliers.ValidFrom AND suppliers.ValidTo

),

suppliers_archive AS (

    SELECT
        suppliers_archive.*
    FROM
        {{ ref('stg_purchasing_suppliers_archive') }} AS suppliers_archive
    WHERE
        (
            suppliers_archive.ValidFrom > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
            suppliers_archive.SupplierCategoryID IN
            (
                SELECT
                    SupplierCategoryID
                FROM
                    changed_supplier_categories
            ) OR
            suppliers_archive.PrimaryContactPersonID IN
            (
                SELECT
                    PersonID
                FROM
                    changed_people
            )
        ) AND
        (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN suppliers_archive.ValidFrom AND suppliers_archive.ValidTo

),

final AS (

    SELECT
        suppliers.*
    FROM
        suppliers
    
    UNION ALL

    SELECT
        suppliers_archive.*
    FROM
        suppliers_archive 

)

SELECT
    *
FROM
    final