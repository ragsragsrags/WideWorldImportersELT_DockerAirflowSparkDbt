WITH customers AS (

    SELECT 
        *
    FROM
        {{ ref('dim_customers') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate