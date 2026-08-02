{{ config(
    materialized='table',
) }}
{{ ttd.ttd(
    source_name='ttd_raw',
    table_name='standard_streams',
    cm360_source_name='cm360_transformed',
    cm360_table_name=env_var('CM360_DIRECT_BUY_TABLE', 'cm360_direct_buy')
) }}
