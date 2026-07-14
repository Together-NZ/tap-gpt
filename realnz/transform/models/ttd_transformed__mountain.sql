{{ config(
    materialized='table',
    schema='ttd_transformed__mountain',
    alias='ttd_transformed__mountain',
) }}

{{ ttd.ttd(
    source_name='ttd_raw__mountain',
    table_name='standard_streams',
    cm360_source_name='cm360_transformed__mountain',
    cm360_table_name='cm360_direct_buy__mountain',
) }}
