{{ config(
    materialized='table',
) }}
with dash_table AS (
SELECT * FROM `kiwibank-main.google_ads_search_transformed__credit_card.google_ads_search__credit_card`
)
{{ dash_table_general_process.dash_table_search_general_process('Choose') }}