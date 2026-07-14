{{ config(
    materialized='table',
    schema='dash_table_search__tourism',
    alias='dash_table_search__tourism',
) }}

WITH dash_table AS (
    (
        SELECT
            conversions,
            date,
            media_cost,
            clicks,
            device,
            impressions,
            campaign_name,
            campaign_id,
            publisher
        FROM {{ source('google_ads_search_transformed__tourism', 'bing_ads_search__tourism') }}
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
        FROM {{ source('google_ads_search_transformed__tourism', 'google_ads_search__tourism') }}
        GROUP BY campaign_name, campaign_id, device, publisher, date
    )
)
{{ dash_table_general_process.dash_table_search_general_process('BOOK') }}
