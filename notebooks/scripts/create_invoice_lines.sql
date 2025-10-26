CREATE TABLE @DestinationTable 
(
    InvoiceLineID INTEGER, 
    InvoiceID INTEGER, 
    StockItemID INTEGER, 
    Description STRING, 
    PackageTypeID INTEGER, 
    Quantity INTEGER, 
    UnitPrice NUMERIC(18, 2), 
    TaxRate NUMERIC(18, 3), 
    TaxAmount NUMERIC(18, 2), 
    LineProfit NUMERIC(18, 2), 
    ExtendedPrice NUMERIC(18, 2), 
    LastEditedBy INTEGER, 
    LastEditedWhen DATETIME, 
    LoadDate DATETIME
)