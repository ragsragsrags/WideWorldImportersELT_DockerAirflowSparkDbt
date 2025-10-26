WITH source AS
(

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Application_StateProvinces_Archive') }}

),

transformed AS (

    SELECT
        source.StateProvinceID,
        source.StateProvinceName,
        source.CountryID,
        source.SalesTerritory,
        source.ValidFrom,
        source.ValidTo
    FROM
        source

)

SELECT 
    *
FROM
    transformed