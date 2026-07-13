{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT SUM(conversions) AS conversions, date, SUM(media_cost) AS media_cost, SUM(clicks) AS clicks, segments_device as device, SUM(impressions) AS impressions, campaign_name, campaign_id, publisher FROM `zeekr-main.google_ads_search_transformed.google_ads_search` GROUP BY campaign_name, campaign_id, device, publisher, date
)
{{ dash_table_general_process.dash_table_search_general_process('INTENT') }}
