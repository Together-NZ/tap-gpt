{{ config(
    materialized='table',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.google_ads_search(
        source_name='google_ads',
        table_name='google_ads_search'
    ) }}
    UNION ALL
    {{ dash_table_general_process.bing_ads_search(
        source_name='google_ads',
        table_name='bing_ads_search'
    ) }}
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
