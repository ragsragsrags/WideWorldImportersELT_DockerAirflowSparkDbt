WITH source AS
(
    
    SELECT
        *
    FROM
        {{ source('WideWorldImportersDW', 'WarehouseHistory') }}

),

final AS 
(

    SELECT
        TableName,
        LoadDate,
        Status,
        Details
    FROM
        source

)

SELECT
    *
FROM
    final