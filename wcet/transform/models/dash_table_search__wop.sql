{{ config(
    materialized='table',
) }}
with dash_table AS (

(
  SELECT SUM(conversions) AS conversions, date,SUM(media_cost) AS media_cost,SUM(clicks) AS clicks, segments_device as device, SUM(impressions) AS impressions,campaign_name,campaign_id,publisher FROM `wcet-main.google_ads_search_transformed__wop.google_ads_search__wop` GROUP BY campaign_name,campaign_id,device,publisher,date
)
)
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}