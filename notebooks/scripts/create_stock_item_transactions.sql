CREATE TABLE @DestinationTable
(
    StockItemTransactionID INTEGER, 
    StockItemID INTEGER, 
    TransactionTypeID INTEGER, 
    CustomerID INTEGER, 
    InvoiceID INTEGER, 
    SupplierID INTEGER, 
    PurchaseOrderID INTEGER, 
    TransactionOccurredWhen DATETIME, 
    Quantity NUMERIC(18, 3), 
    LastEditedBy INTEGER, 
    LastEditedWhen DATETIME, 
    LoadDate DATETIME
)