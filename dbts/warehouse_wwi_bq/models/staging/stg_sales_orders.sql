WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_Orders') }}

),

transformed AS (

    SELECT
        OrderID,
        CustomerID,
        SalespersonPersonID,
        PickedByPersonID,
        ContactPersonID,
        BackorderOrderID,
        CAST(OrderDate AS DATE) AS OrderDate,
        ExpectedDeliveryDate,
        CustomerPurchaseOrderNumber,
        IsUndersupplyBackordered,
        Comments,
        DeliveryInstructions,
        InternalComments,
        CAST(PickingCompletedWhen AS DATE) AS PickingCompletedWhen,
        LastEditedBy,
        LastEditedWhen
    FROM
        source

)

SELECT
    *
FROM
    transformed