{{ config(
    materialized='table',
) }}

{{ hivestack.hivestack(
    table_name='hivestack_raw__twh',
    report_name=env_var('REPORT_NAME', 'warehouse')
) }}
