{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}

SELECT *
FROM {{ source('ga4_central', 'ga4_goal_ecommerce') }}
WHERE campaign_name IN (
    SELECT DISTINCT campaign_name
    FROM {{ source('dash_union_energy', 'dash_union__energy') }}
)
