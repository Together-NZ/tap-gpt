{{ config(
    materialized='table',
) }}

(
{{ google_ads.google_ads_search_keyword(client_id=3244960310) }}
)

UNION ALL

(
{{ google_ads.google_ads_search_keyword(client_id=8385365194) }}
)
