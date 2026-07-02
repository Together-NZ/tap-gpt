{{ config(
    materialized='table',
    alias='ttd_transformed__twh',
) }}

{{ ttd.ttd(
    source_name='ttd_raw__twh',
    table_name='standard_streams',
    cm360_source_name='cm360_transformed__twh',
    cm360_table_name='cm360_direct_buy__twh'
) }}
