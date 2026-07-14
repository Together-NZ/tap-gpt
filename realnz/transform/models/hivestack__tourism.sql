{{ config(
    materialized='table',
    schema='hivestack_transformed__tourism',
    alias='hivestack__tourism',
) }}

{{ hivestack.hivestack(
    table_name='hivestack_raw__tourism',
    report_name=env_var('REPORT_NAME', 'realnz_tourism_report'),
) }}
