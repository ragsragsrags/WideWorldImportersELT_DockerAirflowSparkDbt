WITH source AS
(

    SELECT
        *
    FROM
        {{ source('WideWorldImporters', 'Application_PaymentMethods') }}

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