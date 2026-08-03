{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack(
    table_name='hivestack_raw__protectyourbreath',
    report_name=env_var('REPORT_NAME', 'protectyourbreath_report')
) }}
