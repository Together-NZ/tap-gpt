{{ config(
    materialized='table',
) }}


SELECT * FROM `kiwibank-main.ga4_transformed.ga4_goal_channel_session` WHERE campaign_name IN (
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__ao_social_boosting.dash_union__ao_social_boosting`
)