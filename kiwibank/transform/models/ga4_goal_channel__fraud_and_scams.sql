{{ config(
    materialized='table',
) }}
SELECT * FROM {{ ref('ga4_goal_channel') }}
WHERE campaign_name IN (SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__fraud_and_scams.dash_union__fraud_and_scams`)
