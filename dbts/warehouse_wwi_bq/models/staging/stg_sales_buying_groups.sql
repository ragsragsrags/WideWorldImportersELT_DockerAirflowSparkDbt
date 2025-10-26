WITH source AS (

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Sales_BuyingGroups') }}

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