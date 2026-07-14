{{ config(
    materialized='table',
    alias='google_ads_demand__tourism2'
) }}

{{ google_ads.google_ads_demand(client_id=1476786244) }}
