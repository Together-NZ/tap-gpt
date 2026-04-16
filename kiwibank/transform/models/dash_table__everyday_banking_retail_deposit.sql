{{ config(
    materialized='table',
) }}
SELECT * FROM {{ ref('dash_table') }}
WHERE campaign_name LIKE '%00005%'
