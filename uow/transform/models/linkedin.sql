  {{ config(
    materialized='table',
) }}

WITH 
{{linkedin.campaign(source_name='linkedin_raw', table_name='campaigns')}}
{{linkedin.campaign_campaign_group_link(source_name='linkedin_raw', table_name='campaign_groups')}}
{{linkedin.creative_campaign_link(source_name='linkedin_raw', table_name='creatives')}}
{{linkedin.critical_joining()}}
{{linkedin.daily_stats(source_name='linkedin_raw', table_name='ad_analytics_by_creative')}}
{{linkedin.result_calculation()}}