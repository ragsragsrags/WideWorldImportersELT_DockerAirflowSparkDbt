WITH employees AS (

    SELECT 
        *
    FROM
        {{ ref('dim_employees') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate