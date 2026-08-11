{{ config(
    materialized='table',
    alias='ttd_transformed__noel_leeming',
) }}

{{ ttd.ttd(
    source_name='ttd_raw__noel_leeming',
    table_name='standard_streams',
    cm360_source_name='cm360_transformed__noel_leeming',
    cm360_table_name='cm360_direct_buy__noel_leeming'
) }}
