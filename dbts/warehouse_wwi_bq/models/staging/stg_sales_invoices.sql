WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_Invoices') }}

),

transformed AS (

    SELECT
        InvoiceID,
        CustomerID,
        BillToCustomerID,
        OrderID,
        DeliveryMethodID,
        ContactPersonID,
        AccountsPersonID,
        SalespersonPersonID,
        PackedByPersonID,
        CAST(InvoiceDate AS DATE) AS InvoiceDate,
        CustomerPurchaseOrderNumber,
        IsCreditNote,
        CreditNoteReason,
        Comments,
        DeliveryInstructions,
        InternalComments,
        TotalDryItems,
        TotalChillerItems,
        DeliveryRun,
        RunPosition,
        ReturnedDeliveryData,
        CAST(ConfirmedDeliveryTime AS DATE) AS ConfirmedDeliveryTime,
        ConfirmedReceivedBy,
        LastEditedBy,
        LastEditedWhen
    FROM
        source

)

SELECT
    *
FROM
    transformed