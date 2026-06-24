{{ config(
    materialized='table',
    schema='dv360_transformed__twhs',
    alias='dv360_youtube__twhs',
) }}
{{ dv360.dv360_youtube(
    source_name='dv360_raw__twhs',
    table_name='dv360_youtube',
    dv360_standard_name='dv360_standard__twhs',
    cm360_source_name='cm360_transformed__twhs',
    cm360_table_name='cm360_direct_buy__twhs'
) }}
