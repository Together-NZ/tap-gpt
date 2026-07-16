{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
    alias='google_ads_demand_2'
) }}

{{ google_ads.google_ads_demand(client_id=2950734523) }}
