SELECT
    *
FROM
    {{ ref('__init_dim_dates') }}
ORDER BY
    date