WITH transaction_types AS (

    SELECT 
        *
    FROM
        {{ ref('dim_transaction_types') }}
    LIMIT 1

)

SELECT
    {{ get_cutoff_date() }}  AS LoadDate