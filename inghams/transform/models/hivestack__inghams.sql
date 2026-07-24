{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack(
    table_name='hivestack_raw__inghams',
    report_name=env_var('REPORT_NAME', 'inghams_report')
) }}
