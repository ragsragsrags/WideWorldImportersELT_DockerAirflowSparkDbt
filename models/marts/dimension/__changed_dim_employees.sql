WITH people AS (

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_employees', 
            'stg_application_people', 
            'stg_application_people_archive', 
            'ValidFrom',
            'ValidTo',
            'PersonID' 
        ) 
    }}    

),

final AS (

    SELECT
        *
    FROM
        people
    WHERE
        IsEmployee = TRUE

)

SELECT
    *
FROM 
    final