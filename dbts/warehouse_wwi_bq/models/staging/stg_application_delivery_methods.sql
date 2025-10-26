WITH source AS
(
    
    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Application_DeliveryMethods' ) }}

),

transformed AS
(

    SELECT
        DeliveryMethodID,
        DeliveryMethodName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    * 
FROM 
    transformed