{{ config(
    materialized='table',
    schema=env_var('HIVESTACK_SUFFIX', ''),
) }}

{{ hivestack.hivestack(
    table_name='hivestack_raw',
    report_name='brightr_report'
) }}
