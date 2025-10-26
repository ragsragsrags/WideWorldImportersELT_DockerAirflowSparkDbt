WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_Customers') }}

),

transformed AS (

    SELECT
        CustomerID,
        CustomerName,
        BillToCustomerID,
        CustomerCategoryID,
        BuyingGroupID,
        PrimaryContactPersonID,
        DeliveryCityID,
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