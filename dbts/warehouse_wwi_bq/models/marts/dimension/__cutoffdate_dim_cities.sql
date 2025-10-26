WITH cities AS (

    SELECT 
        *
    FROM
        {{ ref('dim_cities') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate