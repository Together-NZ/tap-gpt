{{ config(
    materialized='table',
) }}
SELECT * FROM {{ ref('ga4_goal_channel') }}
WHERE campaign_name IN (SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__everyday_banking_retail_deposit.dash_union__everyday_banking_retail_deposit`)
