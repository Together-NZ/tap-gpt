{{ config(
    materialized='table',
    schema=env_var('HIVESTACK_SUFFIX', ''),
    alias='hivestack__mountain'
) }}

{{ hivestack.hivestack(
    table_name='hivestack_raw__mountain',
    report_name=env_var('REPORT_NAME', 'realnz_mountain_report')
) }}
