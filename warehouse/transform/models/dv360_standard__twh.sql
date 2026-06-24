{{ config(
    materialized='table',
    schema='dv360_transformed__twh',
    alias='dv360_standard__twh',
) }}
{{ dv360.dv360_standard(
    source_name='dv360_raw__twh',
    table_name='dv360_standard',
    cm360_source_name='cm360_transformed__twh',
    cm360_table_name='cm360_direct_buy__twh'
) }}
