WITH cutoff_dates AS (

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_dim_cities') }} AS last_cutoff_date

),

cities AS (
    SELECT
    	C.CityID,
    	C.CityName,
    	C.Location,
    	C.LatestRecordedPopulation,
    	SP.StateProvinceName,
    	CA.CountryName,
    	CA.Continent,
    	SP.SalesTerritory,
    	CA.Region,
    	CA.Subregion
    FROM
    	(
    		SELECT
    			C.CityID,
    			C.CityName,
    			C.Location,
    			C.LatestRecordedPopulation,
    			C.StateProvinceID
    		FROM
    			{{ source('WideWorldImporters', 'Application_Cities') }} AS C
    		WHERE
    			(SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN C.ValidFrom AND C.ValidTo 
    
    		UNION ALL
    
    		SELECT
    			CA.CityID,
    			CA.CityName,
    			CA.Location,
    			CA.LatestRecordedPopulation,
    			CA.StateProvinceID
    		FROM
    			{{ source('WideWorldImporters', 'Application_Cities_Archive') }} AS CA
    		WHERE
    			(SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN CA.ValidFrom AND CA.ValidTo 
    	) C LEFT JOIN
    	(
    		SELECT
    			SP.StateProvinceID,
    			SP.CountryID,
    			SP.StateProvinceName,
    			SP.SalesTerritory
    		FROM
    			{{ source('WideWorldImporters', 'Application_StateProvinces') }} AS SP
    		WHERE
    			(SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN SP.ValidFrom AND SP.ValidTo 
    
    		UNION ALL
    
    		SELECT
    			SPA.StateProvinceID,
    			SPA.CountryID,
    			SPA.StateProvinceName,
    			SPA.SalesTerritory
    		FROM
    			{{ source('WideWorldImporters', 'Application_StateProvinces_Archive') }} AS SPA
    		WHERE
    			(SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN SPA.ValidFrom AND SPA.ValidTo
    	) SP ON
    		SP.StateProvinceID = C.StateProvinceID LEFT JOIN
    	(
    		SELECT
    			C.CountryID,
    			C.CountryName,
    			C.Continent,
    			C.Region,
    			C.Subregion
    		FROM
    			{{ source('WideWorldImporters', 'Application_Countries') }} AS C
    		WHERE
    			(SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN C.ValidFrom AND C.ValidTo 
    
    		UNION ALL
    
    		SELECT
    			CA.CountryID,
    			CA.CountryName,
    			CA.Continent,
    			CA.Region,
    			CA.Subregion
    		FROM
    			{{ source('WideWorldImporters', 'Application_Countries_Archive') }} CA
    		WHERE
    			(SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN CA.ValidFrom AND CA.ValidTo
    	) CA ON
    		CA.CountryID = SP.CountryID
)


SELECT 
    CityID AS WWICityID,
    CityName AS City,
    StateProvinceName AS StateProvince,
    Location AS Location,
    CountryName AS Country,
    Continent AS Continent,
    SalesTerritory AS SalesTerritory,
    Region AS Region,
    SubRegion AS Subregion,
    LatestRecordedPopulation AS LatestRecordedPopulation
FROM
    cities

UNION ALL

SELECT
    0 AS WWICityID,
    'Unknown' AS City,
    'N/A' AS StateProvince,
    CAST(NULL AS BYTES) AS Location,
    'N/A' AS Country,
    'N/A' AS Continent,
    'N/A' AS SalesTerritory,
    'N/A' As Region,
    'N/A' AS Subregion,
    0 AS LatestRecordedPopulation