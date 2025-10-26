CREATE TABLE @DestinationTable 
(
    PurchaseOrderID INTEGER, 
    SupplierID INTEGER, 
    OrderDate DATE, 
    DeliveryMethodID INTEGER, 
    ContactPersonID INTEGER, 
    ExpectedDeliveryDate DATE, 
    SupplierReference STRING, 
    IsOrderFinalized BOOLEAN, 
    Comments STRING, 
    InternalComments STRING, 
    LastEditedBy INTEGER, 
    LastEditedWhen DATETIME, 
    LoadDate DATETIME
)