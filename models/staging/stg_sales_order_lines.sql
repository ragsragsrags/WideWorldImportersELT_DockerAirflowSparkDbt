WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_OrderLines') }}

),

transformed AS (

    SELECT
        OrderLineID,
        OrderID,
        StockItemID,
        Description,
        PackageTypeID,
        Quantity,
        UnitPrice,
        TaxRate,
        PickedQuantity,
        PickingCompletedWhen,
        ROUND(Quantity * UnitPrice, 2) AS TotalExcludingTax,
        ROUND((Quantity * UnitPrice * TaxRate) / 100.0, 2) AS TaxAmount,
        (
			ROUND(Quantity * UnitPrice, 2) + 
			ROUND((Quantity * UnitPrice * TaxRate) / 100.0, 2)
		) AS TotalIncludingTax,
        LastEditedBy,
        LastEditedWhen
    FROM
        source

)

SELECT
    *
FROM
    transformed