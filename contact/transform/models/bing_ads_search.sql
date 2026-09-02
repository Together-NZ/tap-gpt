{{ config(
    materialized='table',
) }}
{{ google_ads.bing_ads_search_sa360(
    client_id=6806887247,
    sa360_dataset='sa360_contact',
    transfer_id=4291961021,
) }}
