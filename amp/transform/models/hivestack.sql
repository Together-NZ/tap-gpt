{{ config(
    materialized='table',
    schema=env_var('HIVESTACK_SUFFIX', ''),
) }}
{{ hivestack.hivestack('hivestack_raw', env_var('REPORT_NAME', 'amp_report')) }}
