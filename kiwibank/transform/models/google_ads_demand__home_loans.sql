{{ config(
    materialized='table',
) }}
{{ google_ads.google_ads_demand(client_id=1593502440) }}
