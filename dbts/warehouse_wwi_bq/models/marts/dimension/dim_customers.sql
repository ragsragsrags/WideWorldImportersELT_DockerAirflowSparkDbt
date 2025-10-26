WITH customers AS (

    SELECT 
        * 
    FROM 
        {{ ref('__init_dim_customers') }}      

),

buying_groups AS (

    {{ 
        get_table_merged
        (
            'stg_sales_buying_groups', 
            'stg_sales_buying_groups_archive'
        ) 
    }}

),

customer_categories AS (

    {{ 
        get_table_merged
        (
            'stg_sales_customer_categories', 
            'stg_sales_customer_categories_archive'
        ) 
    }}

),

people AS (

    {{ 
        get_table_merged
        (
            'stg_application_people', 
            'stg_application_people_archive'
        ) 
    }}

),

final AS (

    SELECT
        ROW_NUMBER() OVER (ORDER BY customers.CustomerID) AS CustomerKey,
        customers.CustomerID AS WWICustomerID,
        customers.DeliveryCityID AS WWIDeliveryCityID,
        customers.CustomerName AS Customer,
        bill_to_customer.CustomerName AS BillToCustomer,
        customer_categories.CustomerCategoryName AS Category,
        buying_groups.BuyingGroupName AS BuyingGroup,
        people.FullName AS PrimaryContact,
        customers.DeliveryPostalCode AS PostalCode
    FROM
        customers LEFT JOIN
        customers AS bill_to_customer ON
            customers.BillToCustomerID = bill_to_customer.CustomerID LEFT JOIN
        buying_groups ON
            customers.BuyingGroupID = buying_groups.BuyingGroupID LEFT JOIN
        customer_categories ON
            customers.CustomerCategoryID = customer_categories.CustomerCategoryID LEFT JOIN
        people ON
            customers.PrimaryContactPersonID = people.PersonID

    UNION ALL

    SELECT
        0 AS CustomerKey,
        0 AS WWICustomerID,
        0 AS WWIDeliveryCityID,
        'Unknown' AS Customer,
        'N/A' AS BillToCustomer,
        'N/A' AS Category,
        'N/A' AS BuyingGroup,
        'N/A' AS PrimaryContact,
        'N/A' PostalCode 

)

SELECT 
    * 
FROM 
    final
ORDER BY
    CustomerKey