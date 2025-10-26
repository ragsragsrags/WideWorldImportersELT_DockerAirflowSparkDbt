WITH dates AS (

    SELECT 
        *
    FROM
        {{ ref('dim_dates') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate