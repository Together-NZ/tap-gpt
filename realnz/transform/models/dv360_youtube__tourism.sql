{{ config(
    materialized='table',
    schema='dv360_transformed__tourism',
    alias='dv360_youtube__tourism',
) }}

{{ dv360.dv360_youtube(
    source_name='dv360_raw__tourism',
    table_name='dv360_youtube',
    dv360_standard_name='dv360_standard__tourism',
    cm360_source_name='cm360_transformed__tourism',
    cm360_table_name='cm360_direct_buy__tourism',
) }}
