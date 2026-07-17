{{ config(
    materialized='table',
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
    FROM {{ source('google_ads', 'google_ads_search') }}
    GROUP BY campaign_name, campaign_id, device, publisher, date
),
processed AS (
    {{ dash_table_general_process.dash_table_search_general_process('INTENT') }}
)
SELECT
    *,
    CASE
        WHEN LOWER(campaign_name) LIKE '%gi%'
            OR LOWER(campaign_name) LIKE '%general insurance%'
            OR LOWER(campaign_name) LIKE '%car%'
            OR LOWER(campaign_name) LIKE '%home%'
            OR LOWER(campaign_name) LIKE '%content%'
        THEN 'General Insurance'
        ELSE 'Wealth'
    END AS sub_brands
FROM processed
