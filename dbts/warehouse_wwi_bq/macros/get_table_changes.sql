{%- macro get_table_changes(database, cutoffDateTable, mainModel, archiveModel, validFromColumn, validToColumn, keyColumn) -%}
     WITH cutoff_dates AS (

        SELECT
            {{ get_last_cutoff_date(database, cutoffDateTable) }} AS last_cutoff_date,
            {{ get_cutoff_date() }} AS cutoff_date

    ),

    main AS (

        SELECT
            *
        FROM
            {{ ref(mainModel) }} AS main
        WHERE
            main.{{ validFromColumn }} > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) AND
            {%- if archiveModel != "" -%}
            (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN main.{{ validFromColumn }} AND main.{{ validToColumn }}
            {% else %}
            main.{{ validFromColumn }} <= (SELECT cutoff_date FROM cutoff_dates LIMIT 1)
            {%- endif -%}

    ),

    {%- if archiveModel != "" -%}
    archive AS (

        SELECT
            *
        FROM
            {{ ref(archiveModel) }} AS archive
        WHERE
            archive.{{ validFromColumn }} > (SELECT last_cutoff_date FROM cutoff_dates LIMIT 1) AND
            (SELECT cutoff_date FROM cutoff_dates LIMIT 1) BETWEEN archive.{{ validFromColumn }} AND archive.{{ validToColumn }}

    ),

    {%- endif -%}

    final AS (
        
        SELECT
            main.*
        FROM
            main    

        {%- if archiveModel != "" %}
        
        UNION ALL

        SELECT
            archive.*
        FROM
            archive
        
        {%- endif -%}
            
    )

    SELECT 
        *
    FROM
        final

{%- endmacro -%}