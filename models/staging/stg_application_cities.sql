WITH source AS 
(

    SELECT
        *
    FROM
        {{ source("WideWorldImporters", "Application_Cities") }}

),

transformed AS (

    SELECT
        CityID,
        CityName,
        StateProvinceID,
        Location,
        LatestRecordedPopulation,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    *
FROM
    transformed