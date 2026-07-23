{{ config(materialized='table') }}

WITH processed AS (
    WITH dash_table AS (
        {{ dash_table_general_process.google_ads_search(
            source_name='google_ads_central',
            table_name='google_ads_search'
        ) }}
        UNION ALL
        {{ dash_table_general_process.bing_ads_search(
            source_name='google_ads_central',
            table_name='bing_ads_search'
        ) }}
    )
    {{ dash_table_general_process.dash_table_search_general_process('ME') }}
)
SELECT
    * EXCEPT(funnel),
    CASE WHEN LOWER(campaign_name) LIKE '%awareness%' THEN 'WE' ELSE 'ME' END AS funnel
FROM processed
