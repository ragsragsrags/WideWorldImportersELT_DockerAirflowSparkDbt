WITH suppliers AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_dim_suppliers') }}      

),

supplier_categories AS (

    {{ 
        get_table_merged
        (
            'stg_purchasing_supplier_categories', 
            'stg_purchasing_supplier_categories_archive'
        ) 
    }}

),

people AS (

    {{ 
        get_table_merged
        (
            'stg_application_people', 
            'stg_application_people_archive'
        ) 
    }}

),

final AS (
        
    SELECT
        ROW_NUMBER() OVER (ORDER BY suppliers.SupplierID) AS SupplierKey,
        suppliers.SupplierID AS WWISupplierID,
        suppliers.SupplierName AS Supplier,
        supplier_categories.SupplierCategoryName AS Category,
        people.FullName AS PrimaryContact,
        suppliers.SupplierReference,
        suppliers.PaymentDays,
        suppliers.DeliveryPostalCode AS PostalCode
    FROM
        suppliers LEFT JOIN
        supplier_categories ON
            suppliers.SupplierCategoryID = supplier_categories.SupplierCategoryID LEFT JOIN
        people ON
            suppliers.PrimaryContactPersonID = people.PersonID

    UNION ALL

    SELECT
        0 AS SupplierKey,
        0 AS WWISupplierID,
        'Unknown' AS Supplier,
        'N/A' AS Category,
        'N/A' AS PrimaryContact,
        'N/A' AS SupplierReference,
        0 AS PaymentDays,	
        'N/A' AS PostalCode

)

SELECT 
    * 
FROM 
    final