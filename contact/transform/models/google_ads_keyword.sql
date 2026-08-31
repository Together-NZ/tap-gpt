{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['campaign_name_selection', 'channel', 'funnel', 'publisher'],
) }}
{{ google_ads.google_ads_search_keyword(
    client_id=4362586476,
    funnel_expression="CASE WHEN LOWER(campaign_name) LIKE '%awareness%' THEN 'WE' ELSE 'ME' END"
) }}
