SELECT 
    P.PersonID AS WWIEmployeeID,
    P.FullName AS Employee,
    P.PreferredName AS PreferredName,
    P.IsSalesperson AS IsSalesPerson,
    P.Photo
FROM 
    {{ source('WideWorldImporters', 'Application_People') }} AS P 
WHERE
    P.IsEmployee = TRUE AND
    {{ get_cutoff_date() }} BETWEEN P.ValidFrom AND P.ValidTo

UNION ALL

SELECT 
    PA.PersonID,
    PA.FullName,
    PA.PreferredName,
    PA.IsSalesperson,
    PA.Photo
FROM 
    {{ source('WideWorldImporters', 'Application_People_Archive') }} AS PA 
WHERE
    PA.IsEmployee = TRUE AND
    {{ get_cutoff_date() }} BETWEEN PA.ValidFrom AND PA.ValidTo

UNION ALL

SELECT
    0 AS WWIEmployeeID,
    'Unknown' AS Employee,
    'N/A' AS PreferredName,
    FALSE AS IsSalesperson,
    CAST(NULL AS BYTES) AS Photo