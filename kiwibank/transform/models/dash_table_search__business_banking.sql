{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT * FROM `kiwibank-main.google_ads_search_transformed__business_banking.google_ads_search__business_banking`
)
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}