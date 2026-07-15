{{ config(
    materialized='table',
    alias='dash_table_search__mountain'
) }}

WITH dash_table AS (
    SELECT
        SUM(conversions) AS conversions,
        date,
        SUM(media_cost) AS media_cost,
        SUM(clicks) AS clicks,
        segments_device AS device,
        SUM(impressions) AS impressions,
        campaign_name,
        campaign_id,
        publisher
    FROM {{ source('google_ads_search_transformed__mountain', 'google_ads_search__mountain') }}
    GROUP BY campaign_name, campaign_id, device, publisher, date
)
{{ dash_table_general_process.dash_table_search_general_process('BOOK') }}
