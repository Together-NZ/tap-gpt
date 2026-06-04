{{ config(
    materialized='table',
    schema='dv360_transformed__beervana',
    alias='dv360_youtube__beervana',
) }}
{{ dv360.dv360_youtube(
    source_name='dv360_raw__beervana',
    table_name='dv360_youtube',
    dv360_standard_name='dv360_standard__beervana',
    cm360_source_name='cm360_transformed__beervana',
    cm360_table_name='cm360_direct_buy__beervana'
) }}
