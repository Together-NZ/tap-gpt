{{ config(
    materialized='table',
    schema='google_ads_search_transformed__tourism',
    alias='google_ads_demand__tourism',
) }}

{{ google_ads.google_ads_demand(client_id=5773966980) }}
