{{ config(
    materialized='table',
    schema=env_var('OUTBRAIN_SUFFIX', ''),
) }}

WITH {{ outbrain.ad_report(source_name='outbrain_raw', table_name='ad_report') }},
{{ outbrain.ad(source_name='outbrain_raw', table_name='ad') }},
{{ outbrain.campaign(source_name='outbrain_raw', table_name='campaigns') }},
{{ outbrain.final() }}
