{{ config(
    materialized='table',
) }}
{{ dv360.dv360_youtube(
    source_name='dv360_raw__waitoa',
    table_name='dv360_youtube',
    dv360_standard_name='dv360_standard__waitoa',
    cm360_source_name='cm360_transformed__waitoa',
    cm360_table_name='cm360_direct_buy__waitoa'
) }}
