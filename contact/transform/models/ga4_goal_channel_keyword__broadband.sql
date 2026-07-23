{{ config(materialized='table') }}

SELECT *
FROM {{ source('ga4_central', 'ga4_goal_channel_keyword') }}
WHERE campaign_name IN (
    SELECT DISTINCT campaign_name
    FROM {{ source('dash_union_broadband', 'dash_union__broadband') }}
)
