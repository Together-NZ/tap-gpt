{{ config(
    materialized='table',
    schema='google_ads_search_transformed__volvo',
    alias='google_ads_keyword__volvo',
) }}

{{ google_ads.google_ads_search_keyword(client_id=4308178898) }}
