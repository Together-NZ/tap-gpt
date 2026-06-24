{{ config(
    materialized='table',
    schema='dv360_transformed__twhs',
    alias='dv360_standard__twhs',
) }}
{{ dv360.dv360_standard(
    source_name='dv360_raw__twhs',
    table_name='dv360_standard',
    cm360_source_name='cm360_transformed__twhs',
    cm360_table_name='cm360_direct_buy__twhs'
) }}
