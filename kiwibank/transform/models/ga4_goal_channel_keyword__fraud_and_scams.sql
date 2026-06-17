{{ config(
    materialized='table',
) }}

SELECT * FROM `kiwibank-main.ga4_transformed.ga4_goal_channel_keyword` WHERE campaign_name IN (
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__fraud_and_scams.dash_union__fraud_and_scams`
)