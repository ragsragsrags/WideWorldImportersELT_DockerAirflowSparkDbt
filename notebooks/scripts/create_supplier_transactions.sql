CREATE TABLE @DestinationTable 
(
    SupplierTransactionID INTEGER, 
    SupplierID INTEGER, 
    TransactionTypeID INTEGER, 
    PurchaseOrderID INTEGER, 
    PaymentMethodID INTEGER, 
    SupplierInvoiceNumber STRING, 
    TransactionDate DATE, 
    AmountExcludingTax NUMERIC(18, 2), 
    TaxAmount NUMERIC(18, 2), 
    TransactionAmount NUMERIC(18, 2), 
    OutstandingBalance NUMERIC(18, 2), 
    FinalizationDate DATE, 
    IsFinalized BOOLEAN, 
    LastEditedBy INTEGER, 
    LastEditedWhen DATETIME, 
    LoadDate DATETIME
)