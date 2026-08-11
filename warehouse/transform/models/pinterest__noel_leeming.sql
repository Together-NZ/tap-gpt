{{ config(
    materialized='table',
    schema='pinterest_transformed__noel_leeming',
    alias='pinterest__noel_leeming',
) }}

WITH
{{ pinterest.reports(source_name='pinterest_raw__noel_leeming', table_name='reports') }},
{{ pinterest.ad_groups(source_name='pinterest_raw__noel_leeming', table_name='ad_groups') }},
{{ pinterest.campaigns(source_name='pinterest_raw__noel_leeming', table_name='campaigns') }},
{{ pinterest.ads(source_name='pinterest_raw__noel_leeming', table_name='ads') }},
{{ pinterest.final_calculation() }}
