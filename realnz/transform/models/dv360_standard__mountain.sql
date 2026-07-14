{{ config(
    materialized='table',
    alias='dv360_standard__mountain'
) }}

{{ dv360.dv360_standard(
    source_name='dv360_raw__mountain',
    table_name='dv360_standard',
    cm360_source_name='cm360_transformed__mountain',
    cm360_table_name='cm360_direct_buy__mountain'
) }}
