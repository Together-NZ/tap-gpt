{{ config(
    materialized='table',
) }}
{{ dv360.dv360_standard(
    source_name='dv360_raw__inghams',
    table_name='dv360_standard',
    cm360_source_name='cm360_transformed__inghams',
    cm360_table_name='cm360_direct_buy__inghams'
) }}
