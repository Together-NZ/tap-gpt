{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'publisher', 'channel', 'funnel'],
) }}

SELECT * FROM `kiwibank-main.ga4_transformed.ga4_goal_channel_keyword` WHERE campaign_name IN (
    SELECT DISTINCT campaign_name FROM `kiwibank-main.dash_table__ao_social_boosting.dash_union__ao_social_boosting`
)