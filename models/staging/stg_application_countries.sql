WITH source AS
(

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Application_Countries') }}

),

transformed AS (

    SELECT
        CountryID,
        CountryName,
        Continent,
        Region,
        SubRegion,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    *
FROM
    transformed