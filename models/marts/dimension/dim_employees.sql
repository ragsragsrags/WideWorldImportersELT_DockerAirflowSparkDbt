WITH employees AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_dim_employees') }}      

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY employees.PersonID) AS EmployeeKey,
        employees.PersonID AS WWIEmployeeID,
        employees.FullName AS Employee,
        employees.PreferredName,
        employees.IsSalesPerson,
        employees.Photo
    FROM
        employees

    UNION ALL

    SELECT
        0 AS EmployeeKey,
        0 AS WWIEmployeeID,
        'Unknown' AS Employee,
        'N/A' AS PreferredName,
        FALSE AS IsSalesperson,
        CAST(NULL AS BYTES) AS Photo

)

SELECT 
    * 
FROM 
    final