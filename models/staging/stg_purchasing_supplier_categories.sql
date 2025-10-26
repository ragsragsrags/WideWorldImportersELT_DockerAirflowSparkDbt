WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Purchasing_SupplierCategories') }}

),

transformed AS (

    SELECT
        SupplierCategoryID,
        SupplierCategoryName,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT
    *
FROM
    transformed