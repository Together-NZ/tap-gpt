{{ config(materialized='table') }}

SELECT *
FROM {{ source('dash_table', 'dash_union') }}
WHERE LOWER(campaign_name) LIKE '%broadband%'
