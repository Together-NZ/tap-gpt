{{ config(
    materialized='table',
) }}

WITH dash_table AS (
    (
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
        FROM {{ source('google_ads_search_transformed__brand', 'google_ads_search_brand') }}
        GROUP BY campaign_name, campaign_id, device, publisher, date
    )
    UNION ALL
    (
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
        FROM {{ source('google_ads_search_transformed__marketing', 'google_ads_search_marketing') }}
        GROUP BY campaign_name, campaign_id, device, publisher, date
    )
)

{{ dash_table_general_process.dash_table_search_general_process('INTENT') }}
