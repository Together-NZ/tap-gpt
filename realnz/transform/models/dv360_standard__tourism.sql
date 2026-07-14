{{ config(
    materialized='table',
    schema='dv360_transformed__tourism',
    alias='dv360_standard__tourism',
) }}

{{ dv360.dv360_standard(
    source_name='dv360_raw__tourism',
    table_name='dv360_standard',
    cm360_source_name='cm360_transformed__tourism',
    cm360_table_name='cm360_direct_buy__tourism',
) }}
