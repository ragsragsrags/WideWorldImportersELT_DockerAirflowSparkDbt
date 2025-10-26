SELECT
    PM.PaymentMethodID AS WWIPaymentMethodID,
    PM.PaymentMethodName AS PaymentMethod
FROM
    {{ source('WideWorldImporters', 'Application_PaymentMethods') }} AS PM
WHERE
    {{ get_cutoff_date() }} BETWEEN PM.ValidFrom AND PM.ValidTo

UNION ALL

SELECT
    PMA.PaymentMethodID,
    PMA.PaymentMethodName
FROM
    {{ source('WideWorldImporters', 'Application_PaymentMethods_Archive') }} AS PMA
WHERE
    {{ get_cutoff_date() }} BETWEEN PMA.ValidFrom AND PMA.ValidTo

UNION ALL

SELECT
    0,
    'Unknown'