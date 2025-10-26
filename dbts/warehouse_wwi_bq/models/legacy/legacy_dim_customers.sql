SELECT
    C.CustomerID AS WWICustomerID,
    C.CustomerName AS Customer,
    BC.CustomerName AS BillToCustomer,
    C.DeliveryCityID AS WWIDeliveryCityID,
    CC.CustomerCategoryName AS Category,
    BG.BuyingGroupName AS BuyingGroup,
    PA.FullName AS PrimaryContact,
    C.DeliveryPostalCode AS PostalCode
FROM
    (
        SELECT 
            C.CustomerID,
            C.BillToCustomerID,
            C.CustomerCategoryID,
            C.PrimaryContactPersonID,
            C.BuyingGroupID,
            C.CustomerName,
            C.DeliveryPostalCode,
            C.DeliveryCityID
        FROM
            {{ source('WideWorldImporters', 'Sales_Customers') }} AS C
        WHERE
            {{ get_cutoff_date() }} BETWEEN C.ValidFrom AND C.ValidTo

        UNION ALL

        SELECT 
            CA.CustomerID,
            CA.BillToCustomerID,
            CA.CustomerCategoryID,
            CA.PrimaryContactPersonID,
            CA.BuyingGroupID,
            CA.CustomerName,
            CA.DeliveryPostalCode,
            CA.DeliveryCityID
        FROM
            {{ source('WideWorldImporters', 'Sales_Customers_Archive') }} AS CA
        WHERE
            {{ get_cutoff_date() }} BETWEEN CA.ValidFrom AND CA.ValidTo
    ) C LEFT JOIN
    (
        SELECT 
            C.CustomerID,
            C.CustomerName,
            C.DeliveryPostalCode
        FROM
            {{ source('WideWorldImporters', 'Sales_Customers') }} AS C
        WHERE
            {{ get_cutoff_date() }} BETWEEN C.ValidFrom AND C.ValidTo

        UNION ALL

        SELECT 
            CA.CustomerID,
            CA.CustomerName,
            CA.DeliveryPostalCode
        FROM
            {{ source('WideWorldImporters', 'Sales_Customers_Archive') }} AS CA
        WHERE
            {{ get_cutoff_date() }} BETWEEN CA.ValidFrom AND CA.ValidTo
    ) BC ON
        BC.CustomerID = C.BillToCustomerID LEFT JOIN
    (
        SELECT
            CC.CustomerCategoryID,
            CC.CustomerCategoryName
        FROM
            {{ source('WideWorldImporters', 'Sales_CustomerCategories') }} CC 
        WHERE
            {{ get_cutoff_date() }} BETWEEN CC.ValidFrom AND CC.ValidTo

        UNION ALL

        SELECT
            CCA.CustomerCategoryID,
            CCA.CustomerCategoryName
        FROM
            {{ source('WideWorldImporters', 'Sales_CustomerCategories_Archive') }} AS CCA
        WHERE
            {{ get_cutoff_date() }} BETWEEN CCA.ValidFrom AND CCA.ValidTo
    ) CC ON
        CC.CustomerCategoryID = C.CustomerCategoryID LEFT JOIN
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
    ) PA ON
        PA.PersonID = C.PrimaryContactPersonID LEFT JOIN
    (
        SELECT
            BG.BuyingGroupID,
            BG.BuyingGroupName
        FROM
            {{ source('WideWorldImporters', 'Sales_BuyingGroups') }} AS BG 
        WHERE
            {{ get_cutoff_date() }} BETWEEN BG.ValidFrom AND BG.ValidTo

        UNION ALL

        SELECT
            BGA.BuyingGroupID,
            BGA.BuyingGroupName
        FROM
            {{ source('WideWorldImporters', 'Sales_BuyingGroups_Archive') }} BGA
        WHERE
            {{ get_cutoff_date() }} BETWEEN BGA.ValidFrom AND BGA.ValidTo
    ) BG ON
        BG.BuyingGroupID = C.BuyingGroupID

UNION ALL

SELECT
    0 AS WWICustomerID,
    'Unknown' AS Customer,
    'N/A' AS BillToCustomer,
    0 AS WWIDeliveryCityID,
    'N/A' AS Category,
    'N/A' AS BuyingGroup,
    'N/A' AS PrimaryContact,
    'N/A' PostalCode