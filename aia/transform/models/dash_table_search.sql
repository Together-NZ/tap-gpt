{{ config(
    materialized='table',
) }}

WITH dash_table AS (
    {{ dash_table_general_process.google_ads_search(
        source_name='google_ads_search_transformed__brand',
        table_name='google_ads_search_brand'
    ) }}
    UNION ALL
    {{ dash_table_general_process.google_ads_search(
        source_name='google_ads_search_transformed__marketing',
        table_name='google_ads_search_marketing'
    ) }}
)

{{ dash_table_general_process.dash_table_search_general_process('INTENT') }}
