{{ config(
    materialized='table',
) }}

WITH
{{ pinterest.reports(source_name='pinterest_raw', table_name='reports') }},
{{ pinterest.ad_groups(source_name='pinterest_raw', table_name='ad_groups') }},
{{ pinterest.campaigns(source_name='pinterest_raw', table_name='campaigns', campaign_name_filter='colorsteel') }},
{{ pinterest.ads(source_name='pinterest_raw', table_name='ads') }},
{{ pinterest.final_calculation() }}
