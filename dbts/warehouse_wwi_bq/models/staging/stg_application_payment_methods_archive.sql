WITH source AS
(

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Application_PaymentMethods_Archive') }}

),

final AS (

    SELECT
        PaymentMethodID,
        PaymentMethodName,
        ValidFrom,
        ValidTo
    FROM
        source

)

SELECT 
    * 
FROM 
    final