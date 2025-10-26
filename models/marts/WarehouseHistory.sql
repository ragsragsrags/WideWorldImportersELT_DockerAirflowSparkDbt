{{ config(materialized='incremental') }}

WITH cities AS
(

    SELECT
        'dim_cities' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_dim_cities') }} cutoff_date

),

customers AS
(

    SELECT
        'dim_customers' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_dim_cities') }} cutoff_date

),

dates AS
(

    SELECT
        'dim_dates' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_dim_dates') }} cutoff_date

),

employees AS
(

    SELECT
        'dim_employees' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_dim_employees') }} cutoff_date

),

payment_methods AS
(

    SELECT
        'dim_payment_methods' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_dim_payment_methods') }} cutoff_date

),

stock_items AS
(

    SELECT
        'dim_stock_items' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_dim_stock_items') }} cutoff_date

),

suppliers AS
(

    SELECT
        'dim_suppliers' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_dim_suppliers') }} cutoff_date

),

transaction_types AS
(

    SELECT
        'dim_transaction_types' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_dim_transaction_types') }} cutoff_date

),

movements AS
(

    SELECT
        'fct_movements' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_fct_movements') }} cutoff_date

),

orders AS 
(

    SELECT
        'fct_orders' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_fct_orders') }} cutoff_date

),

purchases AS 
(

    SELECT
        'fct_purchases' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_fct_purchases') }} cutoff_date

),

sales AS 
(

    SELECT
        'fct_sales' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_fct_sales') }} cutoff_date

),

stock_holdings AS 
(

    SELECT
        'fct_stock_holdings' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_fct_stock_holdings') }} cutoff_date

),

transactions AS 
(

    SELECT
        'fct_transactions' AS TableName,
        cutoff_date.LoadDate,
        'Successful' AS Status,
        CAST(NULL AS STRING) AS Details
    FROM
        {{ ref('__cutoffdate_fct_transactions') }} cutoff_date

),

final AS
(

    SELECT
        *
    FROM
        cities

    UNION ALL

    SELECT
        *
    FROM
        customers

    UNION ALL

    SELECT
        *
    FROM
        dates

    UNION ALL

    SELECT
        *
    FROM
        employees

    UNION ALL

    SELECT
        *
    FROM
        payment_methods

    UNION ALL

    SELECT
        *
    FROM
        stock_items

    UNION ALL

    SELECT
        *
    FROM
        suppliers

    UNION ALL

    SELECT
        *
    FROM
        transaction_types

    UNION ALL

    SELECT
        *
    FROM
        movements

    UNION ALL

    SELECT
        *
    FROM
        orders

    UNION ALL

    SELECT
        *
    FROM
        purchases

    UNION ALL

    SELECT
        *
    FROM
        sales

    UNION ALL

    SELECT
        *
    FROM
        stock_holdings

    UNION ALL

    SELECT
        *
    FROM
        transactions

)

SELECT
    *
FROM
    final