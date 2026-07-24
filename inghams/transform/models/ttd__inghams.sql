{{ config(
    materialized='table',
) }}
{{ ttd.ttd(
    source_name='ttd_raw__inghams',
    table_name='standard_streams',
    cm360_source_name='cm360_transformed__inghams',
    cm360_table_name='cm360_direct_buy__inghams'
) }}
