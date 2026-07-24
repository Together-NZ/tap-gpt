{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack(
    table_name='hivestack_raw__waitoa',
    report_name=env_var('REPORT_NAME', 'waitoa_report')
) }}
