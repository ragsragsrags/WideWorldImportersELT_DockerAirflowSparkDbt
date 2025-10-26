WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_BuyingGroups_Archive') }}

),

transformed AS (

    SELECT
        BuyingGroupID,
        BuyingGroupName,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT
    *
FROM
    transformed