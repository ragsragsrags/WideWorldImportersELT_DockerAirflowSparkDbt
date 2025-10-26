WITH source AS
(

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Application_People_Archive') }}

),

final AS 
(

    SELECT
        PersonID,
        FullName,
        PreferredName,
        IsEmployee,
        IsSalesPerson,
        Photo,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    * 
FROM 
    final