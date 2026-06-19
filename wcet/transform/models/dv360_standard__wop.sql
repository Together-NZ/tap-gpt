{{ config(
    materialized='table',
    schema='dv360_transformed__wop',
    alias='dv360_standard__wop',
) }}
{{ dv360.dv360_standard(
    source_name='dv360_raw__wop',
    table_name='dv360_standard',
    cm360_source_name='cm360_transformed__wop',
    cm360_table_name='cm360_direct_buy__wop'
) }}
