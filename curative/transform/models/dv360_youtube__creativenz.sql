{{ config(
    materialized='table',
) }}
{{ dv360.dv360_youtube(
    source_name='dv360_raw__creativenz',
    table_name='dv360_youtube',
    dv360_standard_name='dv360_standard__creativenz',
    cm360_source_name='cm360_transformed__creativenz',
    cm360_table_name=env_var('CM360_DIRECT_BUY_TABLE', 'cm360_direct_buy__creativenz')
) }}
