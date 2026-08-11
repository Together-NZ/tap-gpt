{{ config(
    materialized='table',
    alias='dv360_standard__noel_leeming',
) }}
{{ dv360.dv360_standard(
    source_name='dv360_raw__noel_leeming',
    table_name='dv360_standard',
    cm360_source_name='cm360_transformed__noel_leeming',
    cm360_table_name='cm360_direct_buy__noel_leeming'
) }}
