{{ config(
    materialized='table',
    schema=env_var('CM360_SUFFIX', 'cm360_transformed__interislander'),
) }}
{{ cm360.cm360_direct_buy(
    source_name='cm360_raw',
    table_name='cm360_report_stream',
    lower_advertiser_name='interislander'
) }}
