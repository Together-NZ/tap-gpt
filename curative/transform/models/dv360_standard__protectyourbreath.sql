{{ config(
    materialized='table',
) }}
{{ dv360.dv360_standard(
    source_name='dv360_raw__protectyourbreath',
    table_name='dv360_standard',
    cm360_source_name='cm360_transformed__protectyourbreath',
    cm360_table_name=env_var('CM360_DIRECT_BUY_TABLE', 'cm360_direct_buy__protectyourbreath')
) }}
