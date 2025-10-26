{%- macro get_initial_load_date() -%}
    CAST({{ "'" + var('initial_load') + "'" }} AS DATETIME)
{%- endmacro -%}