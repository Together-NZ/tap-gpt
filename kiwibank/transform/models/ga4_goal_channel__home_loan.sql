{{ config(
    materialized='table',
) }}
SELECT * FROM {{ ref('ga4_goal_channel') }}
WHERE campaign_name IN (SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__home_loan.dash_union__home_loan`)
