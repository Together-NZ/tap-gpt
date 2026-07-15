{{ config(
    materialized='table',
    schema=env_var('HIVESTACK_SUFFIX', ''),
    alias='hivestack__tourism'
) }}

{{ hivestack.hivestack(
    table_name='hivestack_raw__tourism',
    report_name=env_var('REPORT_NAME', 'realnz_tourism_report')
) }}
