WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Warehouse_Colors_Archive') }}
    
),

final AS (

    SELECT
        ColorID,
        ColorName,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    * 
FROM 
    final