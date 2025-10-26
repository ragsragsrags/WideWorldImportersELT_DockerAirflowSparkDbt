WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Warehouse_PackageTypes') }}
    
),

final AS (

    SELECT
        PackageTypeID,
        PackageTypeName,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    * 
FROM 
    final