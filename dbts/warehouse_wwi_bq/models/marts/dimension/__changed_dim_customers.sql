WITH cutoff_dates AS (

    SELECT
        {{ get_cutoff_date() }} AS cutoff_date,
        {{ get_last_cutoff_date('WideWorldImportersDW', '__cutoffdate_dim_customers') }} AS last_cutoff_date

),

changed_buying_groups AS 
(

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_customers', 
            'stg_sales_buying_groups', 
            'stg_sales_buying_groups_archive', 
            'ValidFrom',
            'ValidTo',
            'BuyingGroupID'
        ) 
    }}

),

changed_customer_categories AS 
(

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_customers', 
            'stg_sales_customer_categories', 
            'stg_sales_customer_categories_archive', 
            'ValidFrom',
            'ValidTo',
            'CustomerCategoryID'
        ) 
    }}

),

changed_people AS 
(

    {{ 
        get_table_changes
        (
            'WideWorldImportersDW', 
            '__cutoffdate_dim_customers', 
            'stg_application_people', 
            'stg_application_people_archive', 
            'ValidFrom',
            'ValidTo',
            'PersonID'
        ) 
    }}

),

customers AS 
(

    SELECT
        customers.*
    FROM
        {{ ref('stg_sales_customers') }} AS customers
    WHERE
        (
            customers.ValidFrom > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
            customers.BuyingGroupID IN
            (
                SELECT
                    BuyingGroupID
                FROM
                    changed_buying_groups
            ) OR
            customers.CustomerCategoryID IN
            (
                SELECT
                    CustomerCategoryID
                FROM
                    changed_customer_categories
            ) OR
            customers.PrimaryContactPersonID IN
            (
                SELECT
                    PersonID
                FROM
                    changed_people
            )
        ) AND
        (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN customers.ValidFrom AND customers.ValidTo 

),

customers_archive AS 
(

    SELECT
        customers_archive.*
    FROM
        {{ ref('stg_sales_customers_archive') }} AS customers_archive
    WHERE
        (
            customers_archive.ValidFrom > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) OR
            customers_archive.BuyingGroupID IN
            (
                SELECT
                    BuyingGroupID
                FROM
                    changed_buying_groups
            ) OR
            customers_archive.CustomerCategoryID IN
            (
                SELECT
                    CustomerCategoryID
                FROM
                    changed_customer_categories
            ) OR
            customers_archive.PrimaryContactPersonID IN
            (
                SELECT
                    PersonID
                FROM
                    changed_people
            )
        ) AND
        (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN customers_archive.ValidFrom AND customers_archive.ValidTo

),

final AS (

    SELECT
        customers.*
    FROM
        customers
    
    UNION ALL

    SELECT
        customers_archive.*
    FROM
        customers_archive 

)

SELECT 
    *
FROM
    final