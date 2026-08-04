{{ config(
    materialized='table',
) }}
{{ hivestack.hivestack(
    table_name='hivestack_raw__fasd',
    report_name=env_var('REPORT_NAME', 'report')
) }}
