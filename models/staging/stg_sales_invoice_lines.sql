WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_InvoiceLines') }}

),

transformed AS (

    SELECT
        InvoiceLineID,
        InvoiceID,
        StockItemID,
        Description,
        PackageTypeID,
        Quantity,
        UnitPrice,
        TaxRate,
        ExtendedPrice - TaxAmount AS TotalExcludingTax,
        TaxAmount,
        LineProfit,
        ExtendedPrice AS TotalIncludingTax,
        LastEditedBy,
        LastEditedWhen
    FROM
        source

)

SELECT
    *
FROM
    transformed