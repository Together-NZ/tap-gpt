{{ config(materialized='table') }}

SELECT *
FROM {{ source('ga4_central', 'ga4_goal_ecommerce') }}
WHERE campaign_name IN (
    SELECT DISTINCT campaign_name
    FROM {{ source('dash_union_mobile', 'dash_union__mobile') }}
)
