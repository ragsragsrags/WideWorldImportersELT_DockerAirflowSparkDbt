WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Purchasing_PurchaseOrders') }}

),

transformed AS (

    SELECT
        PurchaseOrderID,
        OrderDate,
        SupplierID,
        LastEditedWhen
    FROM
        source

)

SELECT
    *
FROM
    transformed