{{ config(
    materialized='table',
    alias='dv360_youtube__noel_leeming',
) }}
{{ dv360.dv360_youtube(
    source_name='dv360_raw__noel_leeming',
    table_name='dv360_youtube',
    dv360_standard_name='dv360_standard__noel_leeming',
    cm360_source_name='cm360_transformed__noel_leeming',
    cm360_table_name='cm360_direct_buy__noel_leeming'
) }}
