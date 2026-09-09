{{ config(
    materialized='table',
) }}
{{ google_ads.bing_ads_search_sa360(
    client_id=6942648948,
    sa360_dataset='search_ads_360_kiwibank',
    transfer_id=9771479395,
) }}
