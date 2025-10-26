{{ config(materialized='incremental') }}

SELECT
    cutoff_date
FROM
    (
        SELECT
            CAST(NULL AS DATETIME) AS cutoff_date
    )
WHERE
    1 = 2