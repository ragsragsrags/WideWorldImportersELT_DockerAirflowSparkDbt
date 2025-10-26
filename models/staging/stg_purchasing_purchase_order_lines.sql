WITH source AS
(
    
    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Purchasing_PurchaseOrderLines') }}

),

transformed AS (

    SELECT
        PurchaseOrderLineID,
        PurchaseOrderID,
        StockItemID,
        OrderedOuters,
        Description,
        ReceivedOuters,
        PackageTypeID,
        IsOrderLineFinalized,
        LastEditedWhen,
        LoadDate
    FROM
        source

)

SELECT
    *
FROM
    transformed