{{ config(
    materialized='table',
) }}
{{ ttd.ttd(source_name='ttd_raw', table_name='standard_streams',cm360_source_name='cm360_transformed',cm360_table_name='cm360_direct_buy') }}