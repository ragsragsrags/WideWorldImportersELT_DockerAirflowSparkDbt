WITH payment_methods AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_dim_payment_methods') }}      

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY payment_methods.PaymentMethodID) AS PaymentMethodKey,
        payment_methods.PaymentMethodID AS WWIPaymentMethodID,
        payment_methods.PaymentMethodName AS PaymentMethod
    FROM
        payment_methods

    UNION ALL

    SELECT
        0,
        0,
        'Unknown'

)

SELECT 
    * 
FROM 
    final
ORDER BY
    PaymentMethodKey