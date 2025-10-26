WITH changed_dates AS (
  
    SELECT
        *
    FROM
        {{ ref('__changed_dim_dates') }}

),

init_dates AS (

    SELECT
        *
    FROM
        {{ 
            table_exists
            (
                "WideWorldImportersDW", 
                "__init_dim_dates", 
                "CAST(NULL AS DATE) AS Date, 
                CAST(NULL AS INTEGER) AS DayNumber,
                CAST(NULL AS INTEGER) AS Day,
                CAST(NULL AS STRING) AS Month,
                CAST(NULL AS STRING) AS ShortMonth,
                CAST(NULL AS INTEGER) AS CalendarMonthNumber,
                CAST(NULL AS STRING) AS CalendarMonthLabel, 
                CAST(NULL AS INTEGER) AS CalendarYear,
                CAST(NULL AS STRING) AS CalendarYearLabel,
                CAST(NULL AS INTEGER) AS FiscalMonthNumber,
                CAST(NULL AS STRING) AS FiscalMonthLabel,
                CAST(NULL AS INTEGER) AS FiscalYear,
                CAST(NULL AS STRING) AS FiscalYearLabel,
                CAST(NULL AS INTEGER) AS ISOWeekNumber
                "    
            ) 
        }}

),

final AS (

    SELECT
        *
    FROM
        changed_dates

    UNION ALL

    SELECT
        *
    FROM
        init_dates 
    WHERE 
        Date NOT IN (
            SELECT
                Date
            FROM
                changed_dates
        )

)

SELECT 
    * 
FROM
    final

