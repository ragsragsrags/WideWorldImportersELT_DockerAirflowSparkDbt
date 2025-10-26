WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_CustomerCategories') }}

),

transformed AS (

    SELECT
        CustomerCategoryID,
        CustomerCategoryName,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT
    *
FROM
    transformed