{{ config(
    materialized='table',
) }}
{{ google_ads.google_ads_search(client_id=env_var('GOOGLE_ADS_CLIENT_ID_CREDIT_CARD', '0')) }}
