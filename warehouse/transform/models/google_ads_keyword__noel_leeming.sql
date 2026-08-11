{{ config(
    materialized='table',
    schema='google_ads_search_transformed__noel_leeming',
    alias='google_ads_keyword__noel_leeming',
) }}

{{ google_ads.google_ads_search_keyword(client_id=7068886290) }}
