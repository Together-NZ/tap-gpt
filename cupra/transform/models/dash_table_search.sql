{{ config(
    materialized='table',
) }}
WITH dash_table AS (
    SELECT * FROM `cupra-main.google_ads_search_transformed.google_ads_search`
)
SELECT *,
    CASE
        WHEN LOWER(campaign_name) LIKE '%search%' OR (publisher) = 'Search' THEN 'SEARCH'
        WHEN LOWER(campaign_name) LIKE '%pmax%'
            OR LOWER(campaign_name) LIKE '%performance max%'
            OR LOWER(campaign_name) LIKE '%performance-max%' THEN 'PERFORMANCE MAX'
        WHEN LOWER(campaign_name) LIKE '%google%'
            AND (
                LOWER(campaign_name) LIKE '%native%'
                OR LOWER(campaign_name) LIKE '%demand gen%'
            ) THEN 'DEMAND GEN'
        ELSE 'OTHER'
    END AS media_format,
    CASE
        WHEN LOWER(publisher) = 'demand gen' THEN 'Demand Gen'
        ELSE 'Paid Search'
    END AS channel,
    'Consideration' AS funnel,
    NULL AS creative_name,
    NULL AS ad_format,
    NULL AS ad_format_detail,
    NULL AS audience_name,
    NULL AS video_completion,
    NULL AS video_50_completion,
    NULL AS video_25_completion,
    NULL AS video_75_completion,
    NULL AS video_views,
    NULL AS campaign_descr,
    NULL AS creative_descr
FROM dash_table
