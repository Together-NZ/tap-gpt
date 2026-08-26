{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

SELECT *
FROM {{ source('dash_table', 'dash_union') }}
WHERE LOWER(campaign_name) NOT IN (
  SELECT DISTINCT LOWER(campaign_name) FROM {{ source('dash_union_broadband', 'dash_union__broadband') }}
  UNION ALL
  SELECT DISTINCT LOWER(campaign_name) FROM {{ source('dash_union_mobile', 'dash_union__mobile') }}
)