WITH cutoff_dates AS (

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_dim_cities') }} AS last_cutoff_date

),

changed_states AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_cities', 
            'stg_application_stateprovinces', 
            'stg_application_stateprovinces_archive', 
            'ValidFrom',
            'ValidTo',
            'StateProvinceID'
        ) 
    }}

),

changed_countries AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_cities', 
            'stg_application_countries', 
            'stg_application_countries_archive', 
            'ValidFrom',
            'ValidTo',
            'CountryID' 
        ) 
    }}

),

merged_states AS (

    {{ 
        get_table_merged
        (
            'stg_application_stateprovinces', 
            'stg_application_stateprovinces_archive'
        ) 
    }}

),

cities AS (
    
    SELECT
        cities.*
    FROM
        {{ ref('stg_application_cities') }} AS cities join
        merged_states ON
            cities.StateProvinceID = merged_states.StateProvinceID
    WHERE
        (
            cities.ValidFrom > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
            cities.StateProvinceID IN
            (
                SELECT
                    changed_states.StateProvinceID
                FROM
                    changed_states
            ) OR
            merged_states.CountryID IN
            (
                SELECT
                    changed_countries.CountryID
                FROM
                    changed_countries
            )
        ) AND
        (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN cities.ValidFrom AND cities.ValidTo 

),

cities_archive AS
(

    SELECT
        cities_archive.*
    FROM
        {{ ref('stg_application_cities_archive') }} AS cities_archive join
        merged_states ON
            cities_archive.StateProvinceID = merged_states.StateProvinceID
    WHERE
        (
            cities_archive.ValidFrom > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
            cities_archive.StateProvinceID IN
            (
                SELECT
                    changed_states.StateProvinceID
                FROM
                    changed_states
            ) OR
            merged_states.CountryID IN
            (
                SELECT
                    changed_countries.CountryID
                FROM
                    changed_countries
            )
        ) AND
        (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN cities_archive.ValidFrom AND cities_archive.ValidTo 

),

final AS (
    
    SELECT
        *
    FROM
        cities

    UNION ALL

    SELECT
        *
    FROM
        cities_archive

)

SELECT 
    * 
FROM 
    final