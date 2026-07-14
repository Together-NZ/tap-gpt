{{ config(
    materialized='table',
    schema='google_ads_search_transformed__tourism',
    alias='bing_ads_search__tourism',
) }}

{{ google_ads.bing_ads_search_sa360(client_id=1704016095) }}
