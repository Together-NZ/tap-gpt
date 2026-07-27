{{ config(
    materialized='table',
    schema=env_var('TTD_SUFFIX', ''),
) }}
{{ ttd.ttd(
    source_name='ttd_raw__freight',
    table_name='standard_streams',
    cm360_source_name='cm360_transformed__freight',
    cm360_table_name=env_var('CM360_DIRECT_BUY_TABLE', 'cm360_direct_buy__freight')
) }}
