WITH cities AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_dim_cities') }}      

),

states AS (

    {{ 
        get_table_merged
        (
            'stg_application_stateprovinces', 
            'stg_application_stateprovinces_archive'
        ) 
    }}

),

countries AS (

    {{ 
        get_table_merged
        (
            'stg_application_countries', 
            'stg_application_countries_archive'
        ) 
    }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY cities.CityID) AS CityKey,
        cities.CityID AS WWICityID,
        cities.CityName AS City,
        states.StateProvinceName AS StateProvince,
        countries.CountryName AS Country,
        countries.Continent,
        states.SalesTerritory,
        countries.Region,
        countries.Subregion,
        cities.Location,
        cities.LatestRecordedPopulation
    FROM
        cities join
        states ON
            cities.StateProvinceID = states.StateProvinceID JOIN
        countries ON
            states.CountryID = countries.CountryID

    UNION ALL

    SELECT
        0 AS CityKey,
        0 AS WWICityID,
        'Unknown' AS City,
        'N/A' AS StateProvince,
        'N/A' AS Country,
        'N/A' AS Continent,
        'N/A' AS SalesTerritory,
        'N/A' As Region,
        'N/A' AS Subregion,
        CAST(NULL AS BYTES) AS Location,
        0 AS LatestRecordedPopulation

)

SELECT 
    * 
FROM 
    final
ORDER BY
    CityKey