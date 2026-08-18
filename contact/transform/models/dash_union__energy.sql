{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}

SELECT *
FROM {{ source('dash_table', 'dash_union') }}
WHERE LOWER(campaign_name) NOT LIKE '%broadband%'
  AND LOWER(campaign_name) NOT LIKE '%mobile%'
