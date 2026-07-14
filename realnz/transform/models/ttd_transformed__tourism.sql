{{ config(
    materialized='table',
    schema='ttd_transformed__tourism',
    alias='ttd_transformed__tourism',
) }}

{{ ttd.ttd(
    source_name='ttd_raw__tourism',
    table_name='standard_streams',
    cm360_source_name='cm360_transformed__tourism',
    cm360_table_name='cm360_direct_buy__tourism',
) }}
