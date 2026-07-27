{{ config(materialized='table') }}

SELECT *
FROM {{ source('dash_table', 'dash_union') }}
WHERE LOWER(campaign_name) NOT LIKE '%broadband%'
  AND LOWER(campaign_name) NOT LIKE '%mobile%'
