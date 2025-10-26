{%- macro populate_dim_dates(database, cutoff_date_table) -%}
    {% set sql_statement %}
    SELECT 
        DATE_DIFF (
            CAST({{ get_cutoff_date() }} AS DATE),
            CAST({{ get_last_cutoff_date(database, cutoff_date_table) }} AS DATE), 
            DAY
        ) + 1 AS days  
    {% endset %}
    {% set days = dbt_utils.get_single_value(sql_statement) | int %}

    {% set sql_statement_cutoff_date %}
    WITH cutoff_dates AS (
        
        SELECT
            CAST({{ get_cutoff_date() }} AS DATE) AS cutoff_date,
            CAST({{ get_last_cutoff_date(database, cutoff_date_table) }} AS DATE) AS last_cutoff_date

    )

    SELECT 
        last_cutoff_date
    FROM
        cutoff_dates 
    {% endset %}
    {% set last_cutoff_date = dbt_utils.get_single_value(sql_statement_cutoff_date) %}


    WITH dates AS (

        {% for n in range(days) %}
        SELECT 
            DATE_ADD (
                CAST('{{ last_cutoff_date }}' AS DATE),
                INTERVAL {{ n }} DAY
            ) AS Date
            {% if not loop.last %}
            UNION ALL
            {% endif %}
        {% endfor %}  
    )

    SELECT
        Date,
        EXTRACT(DAY FROM Date) AS DayNumber,
        EXTRACT(DAY FROM Date) AS Day,
        FORMAT_DATE('%B', Date) AS Month,
        SUBSTR(FORMAT_DATE('%B', Date), 1, 3) AS ShortMonth,
        EXTRACT(MONTH FROM Date) AS CalendarMonthNumber,
        CONCAT(
            'CY', 
            EXTRACT(YEAR FROM Date), 
            '-',
            SUBSTR(FORMAT_DATE('%B', Date), 1, 3)
        ) AS CalendarMonthLabel,
        EXTRACT(Year FROM Date) AS CalendarYear,
        CONCAT(
            'CY',
            EXTRACT(YEAR FROM Date)
        ) AS CalendarYearLabel,
        CASE 
            WHEN EXTRACT(MONTH FROM Date) IN (11, 12) THEN EXTRACT(MONTH FROM Date) - 10
            ELSE EXTRACT(MONTH FROM Date) + 2
        END AS FiscalMonthNumber,
        CONCAT(
            'FY',
            CAST(
                CASE 
                    WHEN EXTRACT(MONTH FROM Date) IN (11, 12) THEN EXTRACT(YEAR FROM Date) + 1
                    ELSE EXTRACT(YEAR FROM Date)
                END AS STRING
            ),
            '-',
            SUBSTR(FORMAT_DATE('%B', Date), 1, 3)
        ) AS FiscalMonthLabel,
        CASE 
            WHEN EXTRACT(MONTH FROM Date) IN (11, 12) THEN EXTRACT(YEAR FROM Date) + 1
            ELSE EXTRACT(YEAR FROM Date)
        END AS FiscalYear,
        CONCAT(
            'FY',
            CAST(
                CASE 
                    WHEN EXTRACT(MONTH FROM Date) IN (11, 12) THEN EXTRACT(YEAR FROM Date) + 1
                    ELSE EXTRACT(YEAR FROM Date)
                END AS STRING
            )
        ) AS FiscalYearLabel,
        EXTRACT(ISOWEEK FROM Date) AS ISOWeekNumber
    FROM
        dates
{%- endmacro -%}