WITH movements AS (

    SELECT 
        *
    FROM
        {{ ref('fct_movements') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate