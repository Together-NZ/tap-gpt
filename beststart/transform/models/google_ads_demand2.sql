{{ config(
    materialized='table',
    schema=env_var('GOOGLE_ADS_SEARCH_SUFFIX', ''),
) }}

{{ google_ads.google_ads_demand(client_id=2873672970) }}
