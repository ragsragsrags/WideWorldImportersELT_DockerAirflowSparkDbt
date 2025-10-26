SELECT
    S.SupplierID AS WWISupplierID,
    S.SupplierName AS Supplier,
    SC.SupplierCategoryName AS Category,
    P.FullName AS PrimaryContact,
    S.SupplierReference,
    S.PaymentDays,
    S.DeliveryPostalCode AS PostalCode
FROM
    (
        SELECT 
            S.SupplierID,
            S.SupplierCategoryID,
            S.PrimaryContactPersonID,
            S.SupplierName,
            S.SupplierReference,
            S.PaymentDays,
            S.DeliveryPostalCode
        FROM 
            {{ source('WideWorldImporters', 'Purchasing_Suppliers') }} AS S
        WHERE
            {{ get_cutoff_date() }} BETWEEN S.ValidFrom AND S.ValidTo

        UNION ALL

        SELECT
            SA.SupplierID,
            SA.SupplierCategoryID,
            SA.PrimaryContactPersonID,
            SA.SupplierName,
            SA.SupplierReference,
            SA.PaymentDays,
            SA.DeliveryPostalCode
        FROM
            {{ source('WideWorldImporters', 'Purchasing_Suppliers_Archive') }} AS SA
        WHERE
            {{ get_cutoff_date() }} BETWEEN SA.ValidFrom AND SA.ValidTo
    ) S LEFT JOIN
    (
        SELECT 
            SC.SupplierCategoryID,
            SC.SupplierCategoryName
        FROM
            {{ source('WideWorldImporters', 'Purchasing_SupplierCategories') }} AS SC 
        WHERE
            {{ get_cutoff_date() }} BETWEEN SC.ValidFrom AND SC.ValidTo

        UNION ALL

        SELECT
            SCA.SupplierCategoryID,
            SCA.SupplierCategoryName
        FROM
            {{ source('WideWorldImporters', 'Purchasing_SupplierCategories_Archive') }} AS SCA
        WHERE
            {{ get_cutoff_date() }} BETWEEN SCA.ValidFrom AND SCA.ValidTo
    ) SC ON
        SC.SupplierCategoryID = S.SupplierCategoryID LEFT JOIN
    (
        SELECT
            P.PersonID,
            P.FullName
        FROM
            {{ source('WideWorldImporters', 'Application_People') }} AS P
        WHERE
            {{ get_cutoff_date() }} BETWEEN P.ValidFrom AND P.ValidTo

        UNION ALL

        SELECT
            PA.PersonID,
            PA.FullName
        FROM
            {{ source('WideWorldImporters', 'Application_People_Archive') }} AS PA
        WHERE
            {{ get_cutoff_date() }} BETWEEN PA.ValidFrom AND PA.ValidTo
    ) P ON
        P.PersonID = S.PrimaryContactPersonID

UNION ALL

SELECT
    0 AS WWISupplierID,
    'Unknown' AS Supplier,
    'N/A' AS Category,
    'N/A' AS PrimaryContact,
    'N/A' AS SupplierReference,
    0 AS PaymentDays,	
    'N/A' AS PostalCode