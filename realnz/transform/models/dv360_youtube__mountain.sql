{{ config(
    materialized='table',
    alias='dv360_youtube__mountain'
) }}

{{ dv360.dv360_youtube(
    source_name='dv360_raw__mountain',
    table_name='dv360_youtube',
    dv360_standard_name='dv360_standard__mountain',
    cm360_source_name='cm360_transformed__mountain',
    cm360_table_name='cm360_direct_buy__mountain'
) }}
