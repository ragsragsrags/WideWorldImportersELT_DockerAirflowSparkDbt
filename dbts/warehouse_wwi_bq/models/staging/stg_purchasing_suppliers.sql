WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Purchasing_Suppliers') }}

),

transformed AS (

    SELECT
        SupplierID,
        SupplierName,
        SupplierCategoryID,
        PrimaryContactPersonID,
        SupplierReference,
        PaymentDays,
        DeliveryPostalCode,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT
    *
FROM
    transformed