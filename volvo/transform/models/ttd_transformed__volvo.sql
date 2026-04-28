{{ config(
    materialized='table',
) }}
{{ ttd.ttd(source_name='ttd_raw__volvo', table_name='standard_streams',cm360_source_name='cm360_transformed__volvo',cm360_table_name='cm360_direct_buy__volvo') }}