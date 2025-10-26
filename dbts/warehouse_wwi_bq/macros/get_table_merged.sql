{%- macro get_table_merged(mainTable, archiveTable) -%}
    WITH cutoff_date AS (

        SELECT 
            cutoff_date AS latest_cutoff_date
        FROM
            {{ ref("LatestCutoffDate") }}
        LIMIT 1

    ),

    main AS 
    (

        SELECT
            *
        FROM
            {{ ref(mainTable) }} AS main
        WHERE
            (SELECT latest_cutoff_date FROM cutoff_date) BETWEEN ValidFrom AND ValidTo

    ),

    archive AS
    (

        SELECT
            *
        FROM
            {{ ref(archiveTable) }} AS archive
        WHERE
            (SELECT latest_cutoff_date FROM cutoff_date) BETWEEN ValidFrom AND ValidTo

    ),

    final AS (
        
        SELECT
            main.*
        FROM
            main

        UNION ALL

        SELECT
            archive.*
        FROM
            archive

    )

    SELECT 
        *
    FROM
        final

{%- endmacro -%}