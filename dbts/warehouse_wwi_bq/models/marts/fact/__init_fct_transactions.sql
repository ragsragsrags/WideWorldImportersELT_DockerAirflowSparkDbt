WITH changed_transactions AS 
(
  
    SELECT
        *
    FROM
        {{ ref('__changed_fct_transactions') }}

),

init_transactions AS (
    
    SELECT
        *
    FROM
        {{ 
            table_exists_by_stage_view
            (
                "WideWorldImportersDW", 
                "__init_fct_transactions", 
                "__changed_fct_transactions"
            ) 
        }}

),

final AS (

    SELECT
        *
    FROM
        changed_transactions

    UNION ALL

    SELECT
        *
    FROM
        init_transactions
    WHERE
        NOT EXISTS  (
            SELECT
                1
            FROM
                changed_transactions
            WHERE
                IFNULL(changed_transactions.CustomerTransactionID, changed_transactions.SupplierTransactionID) =
                IFNULL(init_transactions.CustomerTransactionID, init_transactions.SupplierTransactionID)
        ) 

)

SELECT 
    * 
FROM 
    final