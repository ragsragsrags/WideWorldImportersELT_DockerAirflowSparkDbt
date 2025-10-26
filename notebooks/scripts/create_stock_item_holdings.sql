CREATE TABLE @DestinationTable 
(
    StockItemID INTEGER, 
    QuantityOnHand INTEGER, 
    BinLocation STRING, 
    LastStocktakeQuantity INTEGER, 
    LastCostPrice NUMERIC(18, 2), 
    ReorderLevel INTEGER, 
    TargetStockLevel INTEGER, 
    LastEditedBy INTEGER, 
    LastEditedWhen DATETIME, 
    LoadDate DATETIME
)