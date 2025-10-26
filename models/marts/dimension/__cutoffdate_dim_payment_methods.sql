WITH payment_methods AS (

    SELECT 
        *
    FROM
        {{ ref('dim_payment_methods') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate