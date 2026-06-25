{{ config(
    materialized='table',
    schema='google_ads_search_transformed__twh',
    alias='google_ads_keyword__twh',
) }}

{{ google_ads.google_ads_search_keyword(client_id=5427952977) }}
