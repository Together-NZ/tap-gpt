{{ config(
    materialized='table',
    schema='dv360_transformed__wop',
    alias='dv360_youtube__wop',
) }}
{{ dv360.dv360_youtube(source_name='dv360_raw__wop', table_name='dv360_youtube', dv360_standard_name='dv360_standard__wop', cm360_source_name='cm360_transformed__wop', cm360_table_name='cm360_direct_buy__wop') }}
